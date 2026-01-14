"""
Call Sender - Proceso independiente para envío de llamadas
Este proceso corre de forma independiente con múltiples workers para máximo throughput.
Replica exactamente la lógica de envío de check_calls.py
Funciona con REDIS + MYSQL para máximo rendimiento.
"""
import asyncio
import time
import json
import sys
import re
import os
import redis
from datetime import datetime, time as dt_time
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor
import threading

# Importar configuración compartida
from shared_config import (
    FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD, GATEWAY,
    DB_URL, RECORDINGS_DIR, GLOBAL_MAX_CONCURRENT_CALLS, CPS_GLOBAL,
    TERMINAL_STATES, CACHE_TTL, CALL_SENDER_WORKERS,
    REDIS_HOST, REDIS_PORT, REDIS_DB,
    get_logger, create_db_engine, is_terminal_state
)

logger = get_logger("call_sender")

# Importar ESL
try:
    import ESL
except ImportError:
    print("Debes instalar los bindings oficiales de FreeSWITCH ESL para Python")
    exit(1)

# Integración con Redis - Conexión directa + RedisCallManager
REDIS_AVAILABLE = False
redis_client = None
redis_manager = None

try:
    # Conexión directa a Redis para operaciones rápidas
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5
    )
    redis_client.ping()
    REDIS_AVAILABLE = True
    logger.info("✅ Redis conectado directamente (alta velocidad)")
    
    # También cargar RedisCallManager para funciones avanzadas
    try:
        from redis_manager import RedisCallManager
        redis_manager = RedisCallManager(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        logger.info("✅ RedisCallManager cargado para CPS y stats")
    except Exception as e:
        logger.warning(f"⚠️ RedisCallManager no disponible: {e}")
        redis_manager = None
        
except redis.ConnectionError as e:
    REDIS_AVAILABLE = False
    redis_client = None
    logger.warning(f"⚠️ Redis no disponible: {e}")
except Exception as e:
    REDIS_AVAILABLE = False
    redis_client = None
    logger.warning(f"⚠️ Error conectando con Redis: {e}")

# WebSocket
try:
    from websocket_server import send_stats_to_websocket, send_event_to_websocket
    WEBSOCKET_AVAILABLE = True
    logger.info("✅ WebSocket disponible")
except ImportError:
    WEBSOCKET_AVAILABLE = False
    logger.info("⚠️ WebSocket no disponible")

# Pool de conexiones
engine = create_db_engine(pool_size=50, max_overflow=25)

# ThreadPoolExecutor para operaciones ESL paralelas
ESL_EXECUTOR = ThreadPoolExecutor(max_workers=30, thread_name_prefix="esl_worker")

# Tracking de UUIDs activos (memoria local, sincronizado con Redis)
GLOBAL_ACTIVE_UUIDS = set()
GLOBAL_UUID_TIMESTAMPS = {}

# Tracking de números activos para evitar llamadas duplicadas
GLOBAL_ACTIVE_NUMBERS = set()  # Set de (campaign_name, numero) activos
GLOBAL_NUMBER_TIMESTAMPS = {}  # Timestamps de cuando se envió cada número

# Caé de configuración de campañas
CAMPAIGN_CONFIG_CACHE = {}

# Lock para operaciones thread-safe
uuid_lock = threading.Lock()

# Flag para controlar el event listener
EVENT_LISTENER_RUNNING = False
EVENT_LISTENER_TASK = None

# Mapeo de Hangup-Cause a estados de llamada
HANGUP_CAUSE_TO_STATE = {
    # Llamada contestada/exitosa
    "NORMAL_CLEARING": "C",  # Completada (puede ser humano o contestador según AMD)
    "ORIGINATOR_CANCEL": "C",  # El sistema colgó (probablemente después de reproducir audio)
    
    # Sin respuesta
    "NO_ANSWER": "N",
    "NO_USER_RESPONSE": "N",
    "PROGRESS_TIMEOUT": "N",
    "RECOVERY_ON_TIMER_EXPIRE": "N",
    
    # Ocupado
    "USER_BUSY": "O",
    "NORMAL_CIRCUIT_CONGESTION": "O",
    
    # Rechazada/Error
    "CALL_REJECTED": "R",
    "USER_NOT_REGISTERED": "R",
    
    # Inválido/No existe
    "UNALLOCATED_NUMBER": "I",
    "INVALID_NUMBER_FORMAT": "I",
    "NO_ROUTE_DESTINATION": "I",
    
    # Error de red/sistema
    "DESTINATION_OUT_OF_ORDER": "E",
    "NETWORK_OUT_OF_ORDER": "E",
    "NORMAL_TEMPORARY_FAILURE": "E",
    "SWITCH_CONGESTION": "E",
    "REQUESTED_CHAN_UNAVAIL": "E",
    "FACILITY_NOT_SUBSCRIBED": "E",
    "OUTGOING_CALL_BARRED": "E",
    "INCOMPATIBLE_DESTINATION": "E",
    "FACILITY_REJECTED": "E",
    "EXCHANGE_ROUTING_ERROR": "E",
    
    # Timeout
    "ALLOTTED_TIMEOUT": "T",
    "MEDIA_TIMEOUT": "T",
    
    # Contestador (si AMD lo detecta)
    "MACHINE_DETECTED": "U",  # AMD detectó máquina
}


def get_state_from_hangup_cause(hangup_cause: str) -> str:
    """Convierte un Hangup-Cause de FreeSWITCH a un estado de llamada"""
    return HANGUP_CAUSE_TO_STATE.get(hangup_cause, "X")  # X = desconocido/otro


async def update_call_state_on_hangup(campaign_name: str, numero: str, uuid: str, 
                                       hangup_cause: str, new_state: str):
    """
    Actualiza el estado de una llamada en la BD cuando termina.
    Solo actualiza si el estado actual no es terminal exitoso (S, C).
    """
    try:
        loop = asyncio.get_event_loop()
        
        def execute_update():
            with engine.begin() as conn:
                # Primero verificar estado actual
                current = conn.execute(text(f"""
                    SELECT estado, intentos FROM `{campaign_name}` 
                    WHERE uuid = :uuid OR telefono = :numero
                    LIMIT 1
                """), {"uuid": uuid, "numero": numero}).fetchone()
                
                if not current:
                    return None
                
                current_state, current_intentos = current[0], current[1] or 0
                
                # No sobrescribir estados exitosos
                if current_state in ('S', 'C'):
                    return current_state
                
                # Actualizar estado y fecha de entrega
                conn.execute(text(f"""
                    UPDATE `{campaign_name}` 
                    SET estado = :estado,
                        fecha_entrega = NOW(),
                        hangup_cause = :hangup_cause
                    WHERE uuid = :uuid OR telefono = :numero
                """), {
                    "estado": new_state,
                    "hangup_cause": hangup_cause,
                    "uuid": uuid,
                    "numero": numero
                })
                return new_state
        
        result = await loop.run_in_executor(ESL_EXECUTOR, execute_update)
        if result:
            logger.debug(f"📝 [{campaign_name}] Estado actualizado: {numero} -> {result} ({hangup_cause})")
        return result
        
    except Exception as e:
        logger.error(f"❌ Error actualizando estado en hangup: {e}")
        return None


async def safe_send_to_websocket(func, *args, **kwargs):
    """Envío seguro al WebSocket"""
    if not WEBSOCKET_AVAILABLE:
        return
    try:
        await func(*args, **kwargs)
    except Exception as e:
        logger.warning(f"⚠️ Error enviando al WebSocket: {e}")


def get_campaign_config(campaign_name, engine_conn):
    """Obtiene configuración de campaña desde caché o BD"""
    current_time = time.time()
    
    # Verificar caché
    if campaign_name in CAMPAIGN_CONFIG_CACHE:
        cache_entry = CAMPAIGN_CONFIG_CACHE[campaign_name]
        if current_time - cache_entry["timestamp"] < CACHE_TTL:
            return cache_entry
    
    # Consultar BD
    try:
        with engine_conn.connect() as conn:
            result = conn.execute(text("""
                SELECT amd, horarios, activo 
                FROM campanas 
                WHERE nombre = :nombre
            """), {"nombre": campaign_name}).fetchone()
            
            if result:
                config = {
                    "amd": result[0] or "PRO",
                    "horarios": result[1],
                    "activo": result[2],
                    "timestamp": current_time
                }
                CAMPAIGN_CONFIG_CACHE[campaign_name] = config
                return config
    except Exception as e:
        logger.debug(f"Error obteniendo config de {campaign_name}: {e}")
    
    return {"amd": "PRO", "horarios": None, "activo": "S", "timestamp": current_time}


def is_now_in_campaign_schedule(horario_str):
    """Verifica si el horario actual está dentro del rango permitido."""
    if not horario_str:
        return True

    try:
        dias, horas = horario_str.split('|')
        day_range = dias.split('-')
        if len(day_range) == 2:
            day_start, day_end = int(day_range[0]), int(day_range[1])
        else:
            day_start = day_end = int(day_range[0])

        hour_start = dt_time(int(horas[:2]), int(horas[2:4]))
        hour_end = dt_time(int(horas[5:7]), int(horas[7:9]))

        now = datetime.now()
        weekday = now.isoweekday()
        now_time = now.time()

        if not (day_start <= weekday <= day_end):
            return False
        if not (hour_start <= now_time <= hour_end):
            return False
        return True
    except Exception as e:
        logger.error(f"Error verificando horario '{horario_str}': {e}")
        return True


def register_uuid(uuid: str, campaign_name: str = None, numero: str = None):
    """Registra un UUID y número como activos"""
    with uuid_lock:
        GLOBAL_ACTIVE_UUIDS.add(uuid)
        GLOBAL_UUID_TIMESTAMPS[uuid] = time.time()
        
        # También registrar el número como activo
        if campaign_name and numero:
            key = (campaign_name, numero)
            GLOBAL_ACTIVE_NUMBERS.add(key)
            GLOBAL_NUMBER_TIMESTAMPS[key] = time.time()


def release_uuid(uuid: str, reason: str = "", campaign_name: str = None, numero: str = None):
    """Libera un UUID y número del tracking"""
    with uuid_lock:
        if uuid in GLOBAL_ACTIVE_UUIDS:
            GLOBAL_ACTIVE_UUIDS.discard(uuid)
            logger.debug(f"🔓 UUID liberado: {uuid[:30]}... ({reason})")
        if uuid in GLOBAL_UUID_TIMESTAMPS:
            del GLOBAL_UUID_TIMESTAMPS[uuid]
        
        # Liberar el número también
        if campaign_name and numero:
            key = (campaign_name, numero)
            GLOBAL_ACTIVE_NUMBERS.discard(key)
            if key in GLOBAL_NUMBER_TIMESTAMPS:
                del GLOBAL_NUMBER_TIMESTAMPS[key]


def is_number_active(campaign_name: str, numero: str) -> bool:
    """Verifica si un número está actualmente en proceso"""
    with uuid_lock:
        key = (campaign_name, numero)
        if key in GLOBAL_ACTIVE_NUMBERS:
            # Verificar si no está obsoleto (más de 2 minutos)
            timestamp = GLOBAL_NUMBER_TIMESTAMPS.get(key, 0)
            if time.time() - timestamp > 120:
                # Número obsoleto, liberar
                GLOBAL_ACTIVE_NUMBERS.discard(key)
                if key in GLOBAL_NUMBER_TIMESTAMPS:
                    del GLOBAL_NUMBER_TIMESTAMPS[key]
                return False
            return True
        return False


def get_active_numbers_count(campaign_name: str = None) -> int:
    """Obtiene el número de números activos"""
    with uuid_lock:
        if campaign_name:
            return sum(1 for (camp, _) in GLOBAL_ACTIVE_NUMBERS if camp == campaign_name)
        return len(GLOBAL_ACTIVE_NUMBERS)


def get_active_uuid_count():
    """Obtiene el número de UUIDs activos"""
    with uuid_lock:
        return len(GLOBAL_ACTIVE_UUIDS)


def get_active_campaigns_count():
    """Obtiene el número de campañas activas"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM campanas 
                WHERE activo = 'S' 
                AND tipo = 'Audio'
                AND (fecha_programada IS NULL OR fecha_programada <= NOW())
            """)).scalar()
            return result or 1
    except Exception as e:
        logger.error(f"Error obteniendo campañas activas: {e}")
        return 1


async def send_originate_batch_parallel(con, batch_data, campaign_name):
    """
    Envía múltiples originates con control de throttle para FreeSWITCH
    
    Args:
        con: Conexión ESL
        batch_data: Lista de tuplas (numero, uuid, originate_str)
        campaign_name: Nombre de la campaña
        
    Returns:
        Lista de tuplas (numero, uuid, success)
    """
    loop = asyncio.get_event_loop()
    results = []
    
    async def send_single_originate(numero, uuid, originate_str):
        try:
            response = await loop.run_in_executor(
                ESL_EXECUTOR,
                lambda: con.api(originate_str)
            )
            
            if response:
                body = response.getBody()
                success = "+OK" in body or "Job-UUID" in body
                
                # Detectar errores de throttle de FreeSWITCH
                is_throttle_error = "DESTINATION_OUT_OF_ORDER" in body or "Throttle" in body
                
                if success:
                    logger.debug(f"📞 [{campaign_name}] Originate OK: {numero}")
                elif is_throttle_error:
                    logger.warning(f"🚨 [{campaign_name}] THROTTLE ERROR para {numero} - FreeSWITCH sobrecargado")
                    # Esperar antes de continuar para permitir que FreeSWITCH se recupere
                    await asyncio.sleep(0.5)
                else:
                    logger.warning(f"⚠️ [{campaign_name}] Originate falló: {numero} - {body}")
                return (numero, uuid, success)
            else:
                logger.warning(f"⚠️ [{campaign_name}] Sin respuesta para {numero}")
                return (numero, uuid, False)
        except Exception as e:
            logger.error(f"❌ Error enviando originate para {numero}: {e}")
            return (numero, uuid, False)
    
    # Enviar originates con throttle controlado
    # FreeSWITCH tiene límite de ~30 nuevas sesiones/segundo (sessions-per-second)
    # Enviamos en mini-batches pequeños con delay para evitar Throttle Error! 61
    MINI_BATCH_SIZE = 5  # Reducido de 10 a 5 para evitar throttle
    MINI_BATCH_DELAY = 0.35  # 350ms entre mini-batches = ~14 calls/sec max (seguro)
    
    tasks = []
    for numero, uuid, originate_str in batch_data:
        tasks.append(send_single_originate(numero, uuid, originate_str))
    
    # Ejecutar en mini-batches con delay entre ellos
    for i in range(0, len(tasks), MINI_BATCH_SIZE):
        batch_tasks = tasks[i:i+MINI_BATCH_SIZE]
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        for result in batch_results:
            if isinstance(result, Exception):
                logger.error(f"Error en batch originate: {result}")
            else:
                results.append(result)
        
        # Delay entre mini-batches para no saturar FreeSWITCH
        if i + MINI_BATCH_SIZE < len(tasks):
            await asyncio.sleep(MINI_BATCH_DELAY)
        
        # Detectar si hay errores de throttle y aplicar backoff adicional
        throttle_errors = sum(1 for r in batch_results if isinstance(r, tuple) and not r[2])
        if throttle_errors > len(batch_tasks) * 0.3:  # >30% errores
            logger.warning(f"🚨 [{campaign_name}] Alto ratio de errores ({throttle_errors}/{len(batch_tasks)}) - aplicando backoff")
            await asyncio.sleep(1.0)  # Backoff adicional de 1 segundo
    
    return results


async def batch_update_uuids_and_status(campaign_name, updates_data):
    """
    Actualiza múltiples UUIDs y estados en una sola transacción.
    También incrementa el contador de intentos.
    """
    if not updates_data:
        return
    
    try:
        loop = asyncio.get_event_loop()
        
        def execute_batch_update():
            with engine.begin() as conn:
                for numero, uuid, estado in updates_data:
                    # Actualizar UUID, estado, fecha_envio e incrementar intentos
                    stmt = text(f"""
                        UPDATE `{campaign_name}` 
                        SET uuid = :uuid, 
                            estado = :estado, 
                            fecha_envio = :fecha_envio,
                            intentos = COALESCE(intentos, 0) + 1
                        WHERE telefono = :numero
                    """)
                    conn.execute(stmt, {
                        "uuid": uuid,
                        "estado": estado,
                        "numero": numero,
                        "fecha_envio": datetime.now()
                    })
        
        await loop.run_in_executor(ESL_EXECUTOR, execute_batch_update)
        logger.debug(f"📝 [{campaign_name}] Batch update: {len(updates_data)} registros (intentos incrementados)")
        
    except Exception as e:
        logger.error(f"❌ Error en batch update para {campaign_name}: {e}")


async def cleanup_stale_uuids():
    """Limpia UUIDs y números que han estado activos por más de 2 minutos"""
    current_time = time.time()
    stale_uuids = []
    stale_numbers = []
    
    with uuid_lock:
        # Limpiar UUIDs obsoletos
        for uuid, timestamp in list(GLOBAL_UUID_TIMESTAMPS.items()):
            if current_time - timestamp > 120:  # 2 minutos
                stale_uuids.append(uuid)
        
        # Limpiar números obsoletos
        for key, timestamp in list(GLOBAL_NUMBER_TIMESTAMPS.items()):
            if current_time - timestamp > 120:  # 2 minutos
                stale_numbers.append(key)
    
    # Liberar UUIDs obsoletos
    for uuid in stale_uuids:
        with uuid_lock:
            GLOBAL_ACTIVE_UUIDS.discard(uuid)
            if uuid in GLOBAL_UUID_TIMESTAMPS:
                del GLOBAL_UUID_TIMESTAMPS[uuid]
    
    # Liberar números obsoletos
    for key in stale_numbers:
        with uuid_lock:
            GLOBAL_ACTIVE_NUMBERS.discard(key)
            if key in GLOBAL_NUMBER_TIMESTAMPS:
                del GLOBAL_NUMBER_TIMESTAMPS[key]
    
    total_cleaned = len(stale_uuids) + len(stale_numbers)
    if total_cleaned > 0:
        logger.debug(f"🧹 Limpiados {len(stale_uuids)} UUIDs y {len(stale_numbers)} números obsoletos")
    
    return total_cleaned


async def validate_pending_calls(campaign_name: str, timeout_seconds: int = 120):
    """
    Segunda validación: Detecta llamadas que quedaron en estado 'pendiente'
    por más tiempo del esperado y las marca como timeout.
    
    Args:
        campaign_name: Nombre de la campaña
        timeout_seconds: Segundos después de los cuales una llamada pendiente se considera huérfana
    
    Returns:
        Número de llamadas actualizadas
    """
    try:
        loop = asyncio.get_event_loop()
        
        def execute_validation():
            updated_count = 0
            with engine.begin() as conn:
                # Buscar llamadas en estado P (procesando) que llevan mucho tiempo sin respuesta
                # y que no están siendo trackeadas activamente
                stale_pending = conn.execute(text(f"""
                    SELECT telefono, uuid, intentos, fecha_envio
                    FROM `{campaign_name}`
                    WHERE estado = 'P'
                    AND fecha_envio IS NOT NULL
                    AND fecha_envio < DATE_SUB(NOW(), INTERVAL :timeout SECOND)
                """), {"timeout": timeout_seconds}).fetchall()
                
                for row in stale_pending:
                    telefono, uuid, intentos, fecha_envio = row[0], row[1], row[2] or 0, row[3]
                    
                    # Verificar si el UUID está activo en memoria
                    is_active = False
                    with uuid_lock:
                        is_active = uuid in GLOBAL_ACTIVE_UUIDS if uuid else False
                    
                    if not is_active:
                        # Marcar como timeout (T) para permitir reintento
                        conn.execute(text(f"""
                            UPDATE `{campaign_name}`
                            SET estado = 'T',
                                fecha_entrega = NOW(),
                                hangup_cause = 'PROCESSING_TIMEOUT'
                            WHERE telefono = :telefono
                            AND estado = 'P'
                        """), {"telefono": telefono})
                        updated_count += 1
                        
                        # Liberar UUID si existe
                        if uuid:
                            with uuid_lock:
                                GLOBAL_ACTIVE_UUIDS.discard(uuid)
                                if uuid in GLOBAL_UUID_TIMESTAMPS:
                                    del GLOBAL_UUID_TIMESTAMPS[uuid]
                
                return updated_count
        
        result = await loop.run_in_executor(ESL_EXECUTOR, execute_validation)
        
        if result > 0:
            logger.info(f"🔍 [{campaign_name}] Segunda validación: {result} llamadas en P marcadas como timeout")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error en validación de pendientes para {campaign_name}: {e}")
        return 0


async def validate_all_campaigns_pending():
    """
    Ejecuta la segunda validación de pendientes para todas las campañas activas.
    """
    try:
        with engine.connect() as conn:
            campaigns = conn.execute(text("""
                SELECT nombre FROM campanas
                WHERE activo IN ('S', 'P')
                AND tipo = 'Audio'
            """)).fetchall()
        
        total_updated = 0
        for row in campaigns:
            campaign_name = row[0]
            # Verificar que la tabla existe
            try:
                with engine.connect() as conn:
                    conn.execute(text(f"SELECT 1 FROM `{campaign_name}` LIMIT 1"))
            except:
                continue
            
            updated = await validate_pending_calls(campaign_name, timeout_seconds=120)
            total_updated += updated
        
        return total_updated
        
    except Exception as e:
        logger.error(f"❌ Error en validación global de pendientes: {e}")
        return 0


class DialerStats:
    """Estadísticas del dialer para una campaña"""
    
    def __init__(self, campaign_name=None):
        self.campaign_name = campaign_name
        self.calls_sent = 0
        self.calls_answered = 0
        self.calls_failed = 0
        self.calls_busy = 0
        self.calls_no_answer = 0
        self.calls_ringing = 0
        self.ringing_numbers = set()
        self.active_numbers = set()
        self.unique_no_answer = set()
        self.unique_failed = set()
        self.unique_busy = set()
        self.unique_answered = set()
        self.unique_sent = set()
        self.cps_current = 0.0
        self.cps_max = 0.0
        self.start_time = None
        self.end_time = None
        self.duration = 0.0

    def print_live_stats(self, campaign_name=None):
        campaign = campaign_name or self.campaign_name or ''
        total_processed = len(self.unique_answered) + len(self.unique_failed) + len(self.unique_busy) + len(self.unique_no_answer)
        pending = max(0, self.calls_sent - total_processed)
        sys.stdout.write(
            f"\r📊 [{campaign}] CPS: {self.cps_current:.2f} (Max: {self.cps_max:.2f}) | "
            f"Enviadas: {self.calls_sent} | Pendientes: {pending} | "
            f"Sonando: {len(self.ringing_numbers)} | Activas: {len(self.active_numbers)} | "
            f"Contestadas: {len(self.unique_answered)} | Fallidas: {len(self.unique_failed)} | "
            f"Ocupadas: {len(self.unique_busy)} | Sin respuesta: {len(self.unique_no_answer)} | "
            f"Procesadas: {total_processed}\n"
        )
        sys.stdout.flush()

    def to_dict(self):
        def extract_number(item):
            try:
                if isinstance(item, (list, tuple)):
                    return item[0]
                return item
            except:
                return str(item)

        total_processed = len(self.unique_answered) + len(self.unique_failed) + len(self.unique_busy) + len(self.unique_no_answer)
        pending = max(0, self.calls_sent - total_processed)

        cps_value = 0.0
        cps_max_value = 0.0
        if REDIS_AVAILABLE and self.campaign_name:
            try:
                # Usar redis_manager si está disponible
                if redis_manager:
                    cps_value = redis_manager.get_cps(self.campaign_name)
                    self.cps_current = cps_value
                    cps_max_value = redis_manager.update_max_cps(self.campaign_name, cps_value)
                    self.cps_max = cps_max_value
                # Fallback: calcular CPS directamente desde redis_client
                elif redis_client:
                    key = f"campaign:{self.campaign_name}:cps_calls"
                    current_time = time.time()
                    window = 3  # 3 segundos
                    min_time = current_time - window
                    call_count = redis_client.zcount(key, min_time, current_time)
                    cps_value = round(call_count / window, 2) if call_count > 0 else 0.0
                    self.cps_current = cps_value
                    # Actualizar max CPS
                    max_key = f"campaign:{self.campaign_name}:cps_max"
                    current_max = float(redis_client.get(max_key) or 0)
                    if cps_value > current_max:
                        redis_client.set(max_key, cps_value, ex=3600)
                        cps_max_value = cps_value
                    else:
                        cps_max_value = current_max
                    self.cps_max = cps_max_value
            except Exception as e:
                logger.debug(f"Error obteniendo CPS desde Redis: {e}")

        if self.start_time:
            self.duration = time.time() - self.start_time

        duration_formatted = ""
        if self.duration > 0:
            minutes = int(self.duration // 60)
            seconds = int(self.duration % 60)
            duration_formatted = f"{minutes}m {seconds}s"

        return {
            "campaign_name": self.campaign_name or "",
            "calls_sent": self.calls_sent,
            "unique_sent": len(self.unique_sent),
            "pending_calls": pending,
            "calls_answered": len(self.unique_answered),
            "calls_failed": len(self.unique_failed),
            "calls_busy": len(self.unique_busy),
            "calls_no_answer": len(self.unique_no_answer),
            "calls_ringing": len(self.ringing_numbers),
            "ringing_numbers": [extract_number(item) for item in self.ringing_numbers],
            "active_calls": len(self.active_numbers),
            "active_numbers": [extract_number(item) for item in self.active_numbers],
            "total_processed": total_processed,
            "cps": cps_value,
            "cps_max": cps_max_value,
            "duration": round(self.duration, 2),
            "duration_formatted": duration_formatted
        }


async def send_calls_for_campaign(campaign_name: str, numbers: list, cps: int, 
                                   max_intentos: int, amd_type: str = None,
                                   stats: DialerStats = None):
    """
    Envía llamadas para una campaña específica
    Replica exactamente la lógica de send_all_calls_persistent de check_calls.py
    """
    if not 1 <= cps <= 100:
        raise ValueError(f"CPS fuera de rango: {cps}")
    
    stats = stats or DialerStats(campaign_name=campaign_name)
    stats.campaign_name = campaign_name
    uuid_map = {}
    delay = 1 / cps
    log = logger.info
    
    log(f"🚦 [{campaign_name}] ===== INICIO DE TAREA DE MARCADO =====")
    log(f"🚦 [{campaign_name}] Enviando llamadas a {cps} CPS con AMD {amd_type or 'PRO'}")
    
    valid_numbers = [n for n in numbers if n.strip().isdigit()]
    log(f"📦 [{campaign_name}] {len(valid_numbers)} números válidos de {len(numbers)} recibidos")
    
    if len(numbers) - len(valid_numbers) > 5:
        invalid_samples = [n for n in numbers[:10] if not n.strip().isdigit()]
        log(f"⚠️ [{campaign_name}] {len(numbers) - len(valid_numbers)} números inválidos. Ejemplos: {invalid_samples[:3]}")
    
    log(f"🌐 [{campaign_name}] UUIDs globales activos: {get_active_uuid_count()}/{GLOBAL_MAX_CONCURRENT_CALLS}")
    
    if len(valid_numbers) == 0:
        log(f"📋 [{campaign_name}] Sin números pendientes para marcar")
        return stats
    
    if WEBSOCKET_AVAILABLE:
        await safe_send_to_websocket(send_stats_to_websocket, {
            "campaign_name": campaign_name,
            "total_numbers": len(valid_numbers),
            "cps": cps,
            **stats.to_dict()
        })
        await asyncio.sleep(1)
    
    # Conexión ESL
    con = ESL.ESLconnection(FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD)
    if not con.connected():
        log(f"❌ [{campaign_name}] No se pudo conectar a FreeSWITCH ESL")
        return stats
    
    log(f"✅ [{campaign_name}] Conectado a FreeSWITCH ESL")
    
    start = time.time()
    stats.start_time = start
    
    # Guardar timestamp de inicio
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE campanas SET fecha_inicio = NOW() WHERE nombre = :nombre
            """), {"nombre": campaign_name})
            log(f"📅 [{campaign_name}] Timestamp de inicio guardado")
    except Exception as e:
        logger.warning(f"⚠️ [{campaign_name}] Error guardando timestamp: {e}")
    
    total = len(valid_numbers)
    i = 0
    
    # Bucle principal de envío
    while i < total:
        # Verificar si campaña sigue activa
        try:
            with engine.connect() as conn:
                campaign_status = conn.execute(
                    text("SELECT activo FROM campanas WHERE nombre = :nombre"),
                    {"nombre": campaign_name}
                ).fetchone()
                
                if not campaign_status:
                    log(f"⛔ [{campaign_name}] Campaña no encontrada - deteniendo")
                    break
                
                if campaign_status[0] != 'S':
                    log(f"⏸️ [{campaign_name}] Campaña no activa (estado: {campaign_status[0]}) - deteniendo")
                    break
        except Exception as e:
            logger.error(f"❌ [{campaign_name}] Error verificando estado: {e}")
            break
        
        # Recalcular CPS dinámicamente
        num_active_campaigns = get_active_campaigns_count()
        cps_adjusted = max(1, CPS_GLOBAL // num_active_campaigns)
        
        if cps_adjusted != cps:
            old_cps = cps
            cps = cps_adjusted
            delay = 1 / cps
            log(f"🔄 [{campaign_name}] CPS AJUSTADO: {old_cps} → {cps}")
        
        # Verificar horario
        try:
            campaign_config = get_campaign_config(campaign_name, engine)
            horario = campaign_config.get("horarios")
            
            if horario and not is_now_in_campaign_schedule(horario):
                log(f"⏸️ [{campaign_name}] Fuera de horario ({horario}) - pausando")
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            text("UPDATE campanas SET activo = 'P' WHERE nombre = :nombre"),
                            {"nombre": campaign_name}
                        )
                    if campaign_name in CAMPAIGN_CONFIG_CACHE:
                        del CAMPAIGN_CONFIG_CACHE[campaign_name]
                except Exception as e:
                    logger.error(f"Error pausando campaña: {e}")
                break
        except Exception as e:
            logger.error(f"Error verificando horario: {e}")
        
        batch = valid_numbers[i:i + cps]
        batch_start_time = time.time()  # Track tiempo de inicio del batch
        
        # Esperar disponibilidad global
        if len(batch) <= GLOBAL_MAX_CONCURRENT_CALLS:
            while True:
                available = GLOBAL_MAX_CONCURRENT_CALLS - get_active_uuid_count()
                if available >= len(batch):
                    break
                logger.info(f"🔒 [{campaign_name}] Esperando plazas: {available}/{len(batch)}")
                await asyncio.sleep(0.5)
        
        log(f"🚩 [{campaign_name}] Enviando lote: {len(batch)} llamadas ({i + len(batch)}/{total})")
        log(f"📊 [{campaign_name}] CPS: {cps}/{CPS_GLOBAL} | Delay: {delay:.3f}s")
        log(f"📡 [{campaign_name}] Plazas: {GLOBAL_MAX_CONCURRENT_CALLS - get_active_uuid_count()}/{GLOBAL_MAX_CONCURRENT_CALLS}")
        
        # Preparar batch de originates
        batch_originates = []
        batch_updates = []
        skipped_count = 0
        
        for numero in batch:
            # VALIDACIÓN 1: Saltar si el número ya está activo en memoria
            if is_number_active(campaign_name, numero):
                logger.debug(f"🚫 [{campaign_name}] Saltando {numero} - ya está activo en memoria")
                skipped_count += 1
                continue
            
            # VALIDACIÓN 2: Verificar en BD que no esté en estado P (procesando)
            try:
                with engine.connect() as conn:
                    check = conn.execute(text(f"""
                        SELECT estado, fecha_envio FROM `{campaign_name}`
                        WHERE telefono = :numero
                        AND estado = 'P'
                        LIMIT 1
                    """), {"numero": numero}).fetchone()
                    
                    if check:
                        logger.debug(f"🚫 [{campaign_name}] Saltando {numero} - ya está procesando (P)")
                        skipped_count += 1
                        continue
            except Exception as e:
                logger.debug(f"Error verificando {numero} en BD: {e}")
            
            uuid = f"{campaign_name}_{numero}_{int(time.time()*1000000)}"
            uuid_map[numero] = uuid
            
            # Throttle: esperar disponibilidad
            timeout_start = time.time()
            while get_active_uuid_count() >= GLOBAL_MAX_CONCURRENT_CALLS:
                if time.time() - timeout_start > 10:
                    logger.warning(f"🚨 [{campaign_name}] Timeout esperando plazas")
                    cleaned = await cleanup_stale_uuids()
                    logger.warning(f"🧹 [{campaign_name}] Limpiados {cleaned} obsoletos")
                    timeout_start = time.time()
                await asyncio.sleep(0.1)
            
            # Registrar UUID y número como activos ANTES de enviar
            register_uuid(uuid, campaign_name, numero)
            
            # IMPORTANTE: Cambiar estado de 'pendiente' a 'P' (procesando) ANTES de enviar
            try:
                with engine.begin() as conn:
                    result = conn.execute(text(f"""
                        UPDATE `{campaign_name}` 
                        SET uuid = :uuid, 
                            estado = 'P',
                            fecha_envio = NOW(),
                            intentos = COALESCE(intentos, 0) + 1
                        WHERE telefono = :numero
                        AND estado IN ('pendiente', 'N', 'E', 'T', 'O', 'R', 'I', 'X', 'U')
                    """), {"uuid": uuid, "numero": numero})
                    
                    if result.rowcount == 0:
                        logger.debug(f"🚫 [{campaign_name}] {numero} ya no está en estado pendiente")
                        release_uuid(uuid, "already_processed", campaign_name, numero)
                        continue
            except Exception as e:
                logger.warning(f"⚠️ Error pre-actualizando {numero}: {e}")
                release_uuid(uuid, "pre_update_failed", campaign_name, numero)
                continue
            
            # Construir originate
            if amd_type and amd_type.upper() == "FREE":
                originate_str = (
                    f"bgapi originate "
                    f"{{ignore_early_media=false,"
                    f"origination_uuid={uuid},"
                    f"campaign_name='{campaign_name}',"
                    f"origination_caller_id_number='{numero}',"
                    f"execute_on_answer='transfer 9999 XML {campaign_name}'}}"
                    f"sofia/gateway/{GATEWAY}/{numero} &park()"
                )
            else:
                originate_str = (
                    f"bgapi originate "
                    f"{{ignore_early_media=false,"
                    f"origination_uuid={uuid},"
                    f"campaign_name='{campaign_name}',"
                    f"origination_caller_id_number='{numero}',"
                    f"execute_on_answer='transfer 9999 XML {campaign_name}'}}"
                    f"sofia/gateway/{GATEWAY}/{numero} 2222 XML DETECT_AMD_PRO"
                )
            
            batch_originates.append((numero, uuid, originate_str))
            # Ya no necesitamos batch_updates porque actualizamos antes
        
        if skipped_count > 0:
            log(f"🚫 [{campaign_name}] Saltados {skipped_count} números (duplicados/activos)")
        
        if not batch_originates:
            log(f"⚠️ [{campaign_name}] Batch vacío después de filtrar duplicados")
            i += cps
            continue
        
        # Envío paralelo
        originate_results = await send_originate_batch_parallel(con, batch_originates, campaign_name)
        
        # Procesar resultados
        successful_sends = 0
        failed_sends = 0
        for numero, uuid, success in originate_results:
            if not success:
                release_uuid(uuid, "originate_failed", campaign_name, numero)
                failed_sends += 1
                # Marcar como error para permitir reintento
                try:
                    with engine.begin() as conn:
                        conn.execute(text(f"""
                            UPDATE `{campaign_name}` 
                            SET estado = 'E', hangup_cause = 'ORIGINATE_FAILED'
                            WHERE uuid = :uuid AND telefono = :numero
                        """), {"uuid": uuid, "numero": numero})
                except:
                    pass
                continue
            
            # La llamada se envió exitosamente, mantiene estado 'P' hasta que llegue el hangup
            successful_sends += 1
            stats.calls_sent += 1
            stats.unique_sent.add((numero, uuid))
            
            # WebSocket notification con stats
            asyncio.create_task(safe_send_to_websocket(send_event_to_websocket, "call_pending", {
                "campaign": campaign_name,
                "campaign_name": campaign_name,
                "numero": numero,
                "uuid": uuid,
                "status": "pending",
                "amd_type": amd_type or "PRO",
                "stats": stats.to_dict()
            }))
        
        # Registrar batch en Redis para CPS
        if REDIS_AVAILABLE and successful_sends > 0:
            try:
                # Método 1: Usar RedisCallManager si está disponible
                if redis_manager:
                    redis_manager.register_calls_sent_batch(campaign_name, successful_sends)
                # Método 2: Usar redis_client directamente
                elif redis_client:
                    current_time = time.time()
                    pipe = redis_client.pipeline()
                    key = f"campaign:{campaign_name}:cps_calls"
                    global_key = "global:cps_calls"
                    for i in range(successful_sends):
                        timestamp = current_time + (i * 0.001)
                        pipe.zadd(key, {str(timestamp): timestamp})
                        pipe.zadd(global_key, {f"{campaign_name}:{timestamp}": timestamp})
                    # Limpiar antiguas (más de 60 segundos)
                    min_time = current_time - 60
                    pipe.zremrangebyscore(key, 0, min_time)
                    pipe.zremrangebyscore(global_key, 0, min_time)
                    pipe.execute()
                logger.debug(f"📊 [{campaign_name}] Registradas {successful_sends} llamadas en Redis para CPS")
            except Exception as e:
                logger.debug(f"⚠️ Error registrando batch en Redis: {e}")
        
        log(f"✅ [{campaign_name}] Batch enviado: {successful_sends}/{len(batch)} exitosos")
        
        # Limpieza periódica
        if i % 50 == 0:
            cleaned = await cleanup_stale_uuids()
            if cleaned > 0:
                log(f"🧹 [{campaign_name}] Limpiados {cleaned} UUIDs obsoletos")
        
        # Delay entre batches - asegurar que cada batch tome suficiente tiempo
        # para respetar el límite de FreeSWITCH (30 sessions/sec default)
        elapsed = time.time() - batch_start_time
        # Mínimo 1.5 segundos por batch para evitar throttle
        remaining_delay = max(0.3, 1.5 - elapsed)
        await asyncio.sleep(remaining_delay)
        i += cps
    
    # Cerrar conexión ESL
    try:
        if con and con.connected():
            con.disconnect()
            log(f"✅ [{campaign_name}] Conexión ESL cerrada")
    except Exception as e:
        log(f"⚠️ [{campaign_name}] Error cerrando ESL: {e}")
    
    # Estadísticas finales
    end = time.time()
    duration = end - start
    stats.end_time = end
    stats.duration = duration
    
    # Guardar duración en BD
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE campanas 
                SET duracion_total = :duracion, fecha_fin = NOW()
                WHERE nombre = :nombre
            """), {"duracion": int(duration), "nombre": campaign_name})
            log(f"⏱️ [{campaign_name}] Duración guardada: {duration:.2f}s")
    except Exception as e:
        logger.warning(f"⚠️ Error guardando duración: {e}")
    
    # Sincronizar stats a MySQL via Redis o directamente
    if REDIS_AVAILABLE:
        try:
            final_stats = stats.to_dict()
            final_stats.update({"campaign_name": campaign_name, "duration": duration})
            
            # Guardar stats en Redis
            if redis_client:
                stats_key = f"campaign:{campaign_name}:stats"
                redis_client.set(stats_key, json.dumps(final_stats), ex=86400)  # TTL 24h
                log(f"📊 [{campaign_name}] Stats guardadas en Redis")
            
            # Guardar en MySQL via RedisCallManager si está disponible
            if redis_manager:
                redis_manager.save_stats_to_mysql(campaign_name, final_stats)
                log(f"📊 [{campaign_name}] Stats sincronizadas a MySQL")
        except Exception as e:
            logger.warning(f"⚠️ Error guardando stats: {e}")
    
    await safe_send_to_websocket(send_event_to_websocket, "campaign_finished", {
        "campaign_name": campaign_name,
        "total_sent": stats.calls_sent,
        "duration": duration,
        "stats": stats.to_dict()
    })
    
    log(f"")
    log(f"{'='*80}")
    log(f"✅ [{campaign_name}] TAREA DE MARCADO FINALIZADA")
    log(f"   📞 Llamadas enviadas: {stats.calls_sent}")
    log(f"   ⏱️  Duración total: {duration:.2f}s")
    log(f"   📊 CPS efectivo: {stats.calls_sent / duration if duration > 0 else 0:.2f}")
    log(f"{'='*80}")
    log(f"")
    
    # Segunda validación: procesar llamadas que quedaron pendientes
    await asyncio.sleep(5)  # Esperar 5 segundos para que lleguen eventos rezagados
    pending_cleaned = await validate_pending_calls(campaign_name, timeout_seconds=60)
    if pending_cleaned > 0:
        log(f"🔍 [{campaign_name}] Segunda validación completada: {pending_cleaned} pendientes procesados")
    
    return stats


async def campaign_sender_worker(worker_id: int, campaign_queue: asyncio.Queue):
    """Worker que procesa campañas de la cola"""
    logger.info(f"🔧 Campaign sender worker #{worker_id} iniciado")
    
    while True:
        try:
            # Obtener campaña de la cola
            try:
                campaign_data = await asyncio.wait_for(campaign_queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                continue
            
            if campaign_data is None:
                logger.info(f"🛑 Worker #{worker_id} terminando")
                break
            
            campaign_name = campaign_data['campaign_name']
            numbers = campaign_data['numbers']
            cps = campaign_data['cps']
            max_intentos = campaign_data['max_intentos']
            amd_type = campaign_data.get('amd_type')
            active_tasks_ref = campaign_data.get('active_tasks_ref')  # Referencia al set de tareas activas
            
            logger.info(f"🚀 Worker #{worker_id} procesando campaña: {campaign_name} ({len(numbers)} números)")
            
            try:
                await send_calls_for_campaign(
                    campaign_name=campaign_name,
                    numbers=numbers,
                    cps=cps,
                    max_intentos=max_intentos,
                    amd_type=amd_type
                )
            except Exception as e:
                logger.error(f"❌ Error en worker #{worker_id} para {campaign_name}: {e}")
            finally:
                # Limpiar de active_tasks cuando termine
                if active_tasks_ref is not None and campaign_name in active_tasks_ref:
                    active_tasks_ref.discard(campaign_name)
                    logger.debug(f"🔓 [{campaign_name}] Removido de active_tasks")
            
            campaign_queue.task_done()
            
        except Exception as e:
            logger.error(f"❌ Error en worker #{worker_id}: {e}")
            await asyncio.sleep(1)


async def get_pending_numbers(campaign_name: str, max_intentos: int):
    """Obtiene números pendientes para una campaña, excluyendo duplicados y activos"""
    try:
        with engine.connect() as conn:
            # Números que pueden marcarse:
            # 1. Estado 'pendiente' = estado inicial, listo para enviar
            # 2. Estados de error que pueden reintentarse (N, E, T, O, R, I, X, U)
            # Excluir: S (exitoso), C (completado), P (procesando actualmente)
            query = text(f"""
                SELECT DISTINCT telefono FROM `{campaign_name}`
                WHERE (
                    -- Estado inicial: pendiente (listo para enviar)
                    estado = 'pendiente'
                    -- Estados de error que pueden reintentarse
                    OR (estado IN ('N', 'E', 'T', 'O', 'R', 'I', 'X', 'U') AND (intentos IS NULL OR intentos < :max_intentos))
                )
                AND estado NOT IN ('S', 'C', 'P')
            """)
            result = conn.execute(query, {"max_intentos": max_intentos})
            potential_numbers = [row[0] for row in result]
            
            logger.info(f"📊 [{campaign_name}] Query encontró {len(potential_numbers)} números disponibles")
            
            # Filtrar números que ya están activos en memoria
            numbers = []
            skipped_active = 0
            for num in potential_numbers:
                if is_number_active(campaign_name, num):
                    skipped_active += 1
                    continue
                numbers.append(num)
            
            if skipped_active > 0:
                logger.debug(f"🚫 [{campaign_name}] Excluidos {skipped_active} números ya activos en memoria")
            
            logger.info(f"📋 [{campaign_name}] Total números para enviar: {len(numbers)}")
            return numbers
            
    except Exception as e:
        logger.error(f"Error obteniendo números para {campaign_name}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []


async def monitor_campaigns(campaign_queue: asyncio.Queue):
    """Monitorea campañas activas y las encola para procesamiento"""
    logger.info("🔍 Iniciando monitor de campañas...")
    
    active_tasks = set()
    
    while True:
        try:
            with engine.connect() as conn:
                # Obtener campañas activas
                campaigns = conn.execute(text("""
                    SELECT nombre, reintentos, horarios
                    FROM campanas
                    WHERE activo = 'S'
                    AND tipo = 'Audio'
                    AND (fecha_programada IS NULL OR fecha_programada <= NOW())
                """)).fetchall()
                
                logger.debug(f"📊 Campañas activas encontradas: {len(campaigns)}")
                for c in campaigns:
                    logger.debug(f"   - {c[0]} (reintentos: {c[1]})")
                
                # Procesar campañas pausadas que entran en horario
                paused = conn.execute(text("""
                    SELECT nombre, reintentos, horarios
                    FROM campanas
                    WHERE activo = 'P'
                    AND tipo = 'Audio'
                    AND (fecha_programada IS NULL OR fecha_programada <= NOW())
                """)).fetchall()
                
                for row in paused:
                    nombre, reintentos, horario = row[0], row[1], row[2] if len(row) > 2 else None
                    if is_now_in_campaign_schedule(horario):
                        logger.info(f"🔄 Reactivando campaña {nombre}")
                        try:
                            with engine.begin() as conn_reactivate:
                                conn_reactivate.execute(
                                    text("UPDATE campanas SET activo = 'S' WHERE nombre = :nombre"),
                                    {"nombre": nombre}
                                )
                        except Exception as e:
                            logger.error(f"Error reactivando {nombre}: {e}")
            
            if not campaigns:
                logger.debug("⚠️ No hay campañas activas con tipo='Audio' y activo='S'")
            
            # Filtrar por horario y existencia
            valid_campaigns = []
            for row in campaigns:
                nombre, reintentos, horario = row[0], row[1], row[2] if len(row) > 2 else None
                
                # Verificar existencia de tabla
                try:
                    with engine.connect() as conn:
                        conn.execute(text(f"SELECT 1 FROM `{nombre}` LIMIT 1"))
                except Exception as e:
                    logger.warning(f"⛔ Tabla {nombre} no existe: {e}")
                    continue
                
                # Verificar horario
                if not is_now_in_campaign_schedule(horario):
                    logger.debug(f"⏸️ {nombre} fuera de horario")
                    continue
                
                valid_campaigns.append((nombre, reintentos or 3))  # Default 3 reintentos
                logger.debug(f"✅ {nombre} válida para procesar")
            
            # Calcular CPS por campaña
            num_active = len(valid_campaigns)
            cps_per_campaign = max(1, CPS_GLOBAL // num_active) if num_active > 0 else 1
            
            logger.debug(f"📊 Campañas válidas: {num_active}, CPS por campaña: {cps_per_campaign}")
            
            # Encolar campañas que no están en proceso
            for campaign_name, max_intentos in valid_campaigns:
                if campaign_name in active_tasks:
                    logger.debug(f"⏳ [{campaign_name}] Ya está en proceso, saltando")
                    continue
                
                # Obtener tipo AMD
                try:
                    with engine.connect() as conn:
                        res = conn.execute(
                            text("SELECT amd FROM campanas WHERE nombre = :nombre"),
                            {"nombre": campaign_name}
                        ).fetchone()
                        amd_type = res[0] if res else "PRO"
                except:
                    amd_type = "PRO"
                
                # Obtener números pendientes
                logger.debug(f"🔍 [{campaign_name}] Buscando números pendientes (max_intentos: {max_intentos})...")
                numbers = await get_pending_numbers(campaign_name, max_intentos)
                
                if numbers:
                    logger.info(f"📋 [{campaign_name}] Encolando {len(numbers)} números (CPS: {cps_per_campaign})")
                    
                    active_tasks.add(campaign_name)
                    
                    await campaign_queue.put({
                        'campaign_name': campaign_name,
                        'numbers': numbers,
                        'cps': cps_per_campaign,
                        'max_intentos': max_intentos,
                        'amd_type': amd_type,
                        'active_tasks_ref': active_tasks  # Pasar referencia para limpieza
                    })
                else:
                    # Verificar si debe finalizarse
                    try:
                        with engine.connect() as conn:
                            stats = conn.execute(text(f"""
                                SELECT
                                    COUNT(CASE WHEN estado IN ('P', 'S') THEN 1 END) as active,
                                    COUNT(*) as total,
                                    COUNT(CASE WHEN estado IN ('C', 'E', 'O', 'N', 'U', 'R', 'I', 'X', 'T') THEN 1 END) as completed
                                FROM `{campaign_name}`
                            """)).fetchone()
                            
                            if stats[0] == 0 and stats[2] >= stats[1] and stats[1] > 0:
                                logger.info(f"🏁 Finalizando {campaign_name} automáticamente")
                                with engine.begin() as conn:
                                    conn.execute(
                                        text("UPDATE campanas SET activo = 'F' WHERE nombre = :nombre"),
                                        {"nombre": campaign_name}
                                    )
                    except Exception as e:
                        logger.debug(f"Error verificando finalización: {e}")
            
            # Limpiar tareas completadas
            # (En una implementación real, necesitarías tracking más sofisticado)
            
            await asyncio.sleep(5)
            
        except Exception as e:
            logger.error(f"❌ Error en monitor: {e}")
            await asyncio.sleep(5)


async def esl_event_listener():
    """
    Listener de eventos ESL para detectar hangups y liberar UUIDs.
    Corre en un hilo separado y procesa eventos de FreeSWITCH.
    """
    global EVENT_LISTENER_RUNNING
    EVENT_LISTENER_RUNNING = True
    
    logger.info("🎧 Iniciando ESL Event Listener para detección de hangups...")
    
    loop = asyncio.get_event_loop()
    reconnect_delay = 1
    max_reconnect_delay = 30
    
    while EVENT_LISTENER_RUNNING:
        con = None
        try:
            # Conexión ESL en modo eventos
            con = ESL.ESLconnection(FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD)
            
            if not con.connected():
                logger.warning(f"⚠️ ESL Event Listener: No se pudo conectar, reintentando en {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                continue
            
            logger.info("✅ ESL Event Listener conectado a FreeSWITCH")
            reconnect_delay = 1  # Reset delay on successful connection
            
            # Suscribirse a eventos de hangup
            con.events("plain", "CHANNEL_HANGUP CHANNEL_HANGUP_COMPLETE CHANNEL_DESTROY")
            logger.info("📡 Suscrito a eventos: CHANNEL_HANGUP, CHANNEL_HANGUP_COMPLETE, CHANNEL_DESTROY")
            
            # Bucle de recepción de eventos
            while EVENT_LISTENER_RUNNING and con.connected():
                # Recibir evento con timeout
                event = await loop.run_in_executor(
                    ESL_EXECUTOR,
                    lambda: con.recvEventTimed(1000)  # 1 segundo timeout
                )
                
                if not event:
                    continue
                
                event_name = event.getHeader("Event-Name")
                
                if event_name in ["CHANNEL_HANGUP", "CHANNEL_HANGUP_COMPLETE", "CHANNEL_DESTROY"]:
                    uuid = event.getHeader("Unique-ID") or event.getHeader("variable_origination_uuid")
                    hangup_cause = event.getHeader("Hangup-Cause") or "UNKNOWN"
                    caller_id = event.getHeader("Caller-Caller-ID-Number") or event.getHeader("variable_origination_caller_id_number") or "N/A"
                    campaign = event.getHeader("variable_campaign_name") or "N/A"
                    
                    # Obtener información adicional de AMD si está disponible
                    amd_result = event.getHeader("variable_amd_result") or event.getHeader("variable_amd_status")
                    
                    if uuid:
                        # Verificar si es un UUID que estamos trackeando
                        is_tracked = False
                        with uuid_lock:
                            is_tracked = uuid in GLOBAL_ACTIVE_UUIDS
                        
                        if is_tracked:
                            # Liberar UUID y número del tracking
                            release_uuid(uuid, f"hangup:{hangup_cause}", campaign if campaign != "N/A" else None, caller_id if caller_id != "N/A" else None)
                            
                            # Determinar estado basado en hangup_cause y AMD
                            new_state = get_state_from_hangup_cause(hangup_cause)
                            
                            # Si AMD detectó máquina, marcar como U (contestador)
                            if amd_result and "MACHINE" in amd_result.upper():
                                new_state = "U"
                            # Si AMD detectó humano y la llamada terminó normal, marcar como S (exitosa)
                            elif amd_result and "HUMAN" in amd_result.upper() and hangup_cause == "NORMAL_CLEARING":
                                new_state = "S"
                            
                            logger.info(f"📴 [{campaign}] Hangup: {caller_id} -> Estado: {new_state} (Causa: {hangup_cause}, AMD: {amd_result or 'N/A'})")
                            
                            # Actualizar estado en BD
                            if campaign and campaign != "N/A":
                                asyncio.create_task(
                                    update_call_state_on_hangup(campaign, caller_id, uuid, hangup_cause, new_state)
                                )
                            
                            # Notificar via WebSocket si está disponible
                            if WEBSOCKET_AVAILABLE:
                                # Obtener stats actualizadas para enviar
                                call_stats = {
                                    "campaign_name": campaign,
                                    "calls_by_state": {}
                                }
                                # Intentar obtener stats desde Redis
                                if REDIS_AVAILABLE and redis_manager:
                                    try:
                                        redis_stats = redis_manager.get_campaign_stats(campaign)
                                        if redis_stats:
                                            call_stats = redis_stats
                                    except:
                                        pass
                                
                                asyncio.create_task(safe_send_to_websocket(
                                    send_event_to_websocket, 
                                    "call_hangup", 
                                    {
                                        "campaign": campaign,
                                        "campaign_name": campaign,
                                        "numero": caller_id,
                                        "uuid": uuid,
                                        "hangup_cause": hangup_cause,
                                        "new_state": new_state,
                                        "amd_result": amd_result,
                                        "event": event_name,
                                        "stats": call_stats
                                    }
                                ))
                            
                            # Actualizar Redis si está disponible
                            if REDIS_AVAILABLE and redis_client:
                                try:
                                    redis_client.srem(f"active_uuids:{campaign}", uuid)
                                    redis_client.srem("global:active_uuids", uuid)
                                except Exception as e:
                                    logger.debug(f"Error actualizando Redis en hangup: {e}")
            
            logger.warning("⚠️ ESL Event Listener: Conexión perdida")
            
        except Exception as e:
            logger.error(f"❌ Error en ESL Event Listener: {e}")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
        finally:
            if con:
                try:
                    con.disconnect()
                except:
                    pass
    
    logger.info("🛑 ESL Event Listener detenido")


async def stop_event_listener():
    """Detiene el event listener de forma segura"""
    global EVENT_LISTENER_RUNNING, EVENT_LISTENER_TASK
    EVENT_LISTENER_RUNNING = False
    if EVENT_LISTENER_TASK:
        EVENT_LISTENER_TASK.cancel()
        try:
            await EVENT_LISTENER_TASK
        except asyncio.CancelledError:
            pass
    logger.info("✅ ESL Event Listener detenido")


async def periodic_cleanup():
    """Limpieza periódica de UUIDs obsoletos y validación de pendientes"""
    cleanup_counter = 0
    
    while True:
        try:
            await asyncio.sleep(30)
            cleanup_counter += 1
            
            # Limpiar UUIDs y números obsoletos
            cleaned = await cleanup_stale_uuids()
            
            if cleaned > 0:
                logger.info(f"🧹 Limpieza: {cleaned} elementos obsoletos")
            
            active_uuids = get_active_uuid_count()
            active_numbers = get_active_numbers_count()
            if active_uuids > 0 or active_numbers > 0:
                logger.info(f"📊 Activos: {active_uuids} UUIDs, {active_numbers} números / {GLOBAL_MAX_CONCURRENT_CALLS} max")
            
            # Cada 2 minutos (4 ciclos de 30s), ejecutar segunda validación de pendientes
            if cleanup_counter % 4 == 0:
                pending_updated = await validate_all_campaigns_pending()
                if pending_updated > 0:
                    logger.info(f"🔍 Validación periódica: {pending_updated} pendientes procesados en total")
                
        except Exception as e:
            logger.error(f"❌ Error en limpieza: {e}")


async def main():
    """Función principal del proceso de envío de llamadas"""
    logger.info("=" * 60)
    logger.info("🚀 CALL SENDER - Proceso independiente")
    logger.info("=" * 60)
    
    # Mostrar estado de conexiones
    logger.info("")
    logger.info("📡 Estado de conexiones:")
    logger.info(f"   MySQL: ✅ Configurado ({DB_URL.split('@')[1] if '@' in DB_URL else DB_URL})")
    if REDIS_AVAILABLE:
        logger.info(f"   Redis: ✅ Conectado ({REDIS_HOST}:{REDIS_PORT})")
        if redis_manager:
            logger.info(f"   RedisCallManager: ✅ Disponible (CPS + Stats)")
        else:
            logger.info(f"   RedisCallManager: ⚠️ No disponible (usando redis_client)")
    else:
        logger.info(f"   Redis: ❌ No disponible")
    logger.info(f"   FreeSWITCH: {FREESWITCH_HOST}:{FREESWITCH_PORT}")
    logger.info(f"   Gateway: {GATEWAY}")
    logger.info("")
    
    # Cola de campañas
    campaign_queue = asyncio.Queue(maxsize=100)
    
    # Iniciar workers
    logger.info(f"🔧 Iniciando {CALL_SENDER_WORKERS} workers de envío...")
    workers = []
    for i in range(CALL_SENDER_WORKERS):
        worker = asyncio.create_task(campaign_sender_worker(i, campaign_queue))
        workers.append(worker)
    logger.info(f"✅ {CALL_SENDER_WORKERS} workers iniciados")
    
    # Iniciar monitor de campañas
    monitor_task = asyncio.create_task(monitor_campaigns(campaign_queue))
    
    # Iniciar limpieza periódica
    cleanup_task = asyncio.create_task(periodic_cleanup())
    
    # Iniciar ESL Event Listener para detección de hangups
    global EVENT_LISTENER_TASK
    EVENT_LISTENER_TASK = asyncio.create_task(esl_event_listener())
    logger.info("✅ ESL Event Listener iniciado para detección de hangups")
    
    logger.info("")
    logger.info("🎯 Sistema listo - Monitoreando campañas activas...")
    logger.info("💡 Presiona Ctrl+C para detener")
    logger.info("")
    
    try:
        await asyncio.gather(monitor_task, cleanup_task, EVENT_LISTENER_TASK, *workers)
    except KeyboardInterrupt:
        logger.info("\n⏹️ Deteniendo call sender...")
    finally:
        # Detener event listener
        await stop_event_listener()
        
        # Detener workers
        for _ in range(CALL_SENDER_WORKERS):
            await campaign_queue.put(None)
        await asyncio.gather(*workers, return_exceptions=True)
        logger.info("✅ Workers detenidos")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️ Call Sender detenido")
    except Exception as e:
        logger.error(f"❌ Error fatal: {e}")
        sys.exit(1)
