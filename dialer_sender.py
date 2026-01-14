"""
Dialer Sender - Proceso independiente para marcador predictivo
Este proceso maneja campañas de tipo "Discador" con transferencia a colas de agentes.
Usa FreeSWITCH ESL para originar llamadas y transferirlas a colas.

La diferencia con call_sender.py (Audio):
- Audio: Origina y reproduce un mensaje automático
- Discador: Origina, detecta humano y transfiere a cola de agentes

El transfer se hace en el dialplan de FreeSWITCH.
"""
import asyncio
import time
import json
import sys
import threading
import traceback
import redis
from datetime import datetime, time as dt_time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor

# Importar configuración compartida
from shared_config import (
    FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD, GATEWAY,
    DB_URL, RECORDINGS_DIR,
    DIALER_MAX_CONCURRENT_CALLS, DIALER_CPS_GLOBAL, DIALER_SENDER_WORKERS,
    DIALER_OVERDIAL_RATIO, DIALER_MIN_AGENTS, DIALER_ABANDON_THRESHOLD,
    DIALER_DEFAULT_QUEUE, DIALER_DEFAULT_CONTEXT,
    DIALER_STATES, DIALER_TERMINAL_STATES,
    CACHE_TTL, REDIS_HOST, REDIS_PORT, REDIS_DB,
    get_logger, create_db_engine, CampaignType
)

logger = get_logger("dialer_sender")

# Importar ESL
try:
    import ESL
except ImportError:
    print("Debes instalar los bindings oficiales de FreeSWITCH ESL para Python")
    exit(1)

# Integración con Redis
REDIS_AVAILABLE = False
redis_client = None

try:
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
    logger.info("✅ Redis conectado")
except Exception as e:
    REDIS_AVAILABLE = False
    redis_client = None
    logger.warning(f"⚠️ Redis no disponible: {e}")

# WebSocket
try:
    from websocket_server import send_stats_to_websocket, send_event_to_websocket
    WEBSOCKET_AVAILABLE = True
    logger.info("✅ WebSocket disponible")
except ImportError:
    WEBSOCKET_AVAILABLE = False
    logger.info("⚠️ WebSocket no disponible")

# Pool de conexiones BD
engine = create_db_engine(pool_size=50, max_overflow=25)

# ThreadPoolExecutor para operaciones ESL paralelas
ESL_EXECUTOR = ThreadPoolExecutor(max_workers=20, thread_name_prefix="dialer_esl")

# Tracking de llamadas activas
ACTIVE_UUIDS: Set[str] = set()
UUID_TIMESTAMPS: Dict[str, float] = {}
ACTIVE_NUMBERS: Set[Tuple[str, str]] = set()  # (campaign_name, numero)
NUMBER_TIMESTAMPS: Dict[Tuple[str, str], float] = {}

# Tracking de agentes
AGENTS_CACHE: Dict[str, dict] = {}  # campaign -> {available, busy, total, last_update}

# Caché de configuración
CAMPAIGN_CONFIG_CACHE: Dict[str, dict] = {}

# Lock para operaciones thread-safe
lock = threading.Lock()

# Control de event listener
EVENT_LISTENER_RUNNING = False
EVENT_LISTENER_TASK = None

# Mapeo de Hangup-Cause a estados del Discador
HANGUP_CAUSE_TO_DIALER_STATE = {
    # Completada exitosamente
    "NORMAL_CLEARING": "C",
    "ORIGINATOR_CANCEL": "C",
    
    # Sin respuesta
    "NO_ANSWER": "N",
    "NO_USER_RESPONSE": "N",
    "PROGRESS_TIMEOUT": "N",
    "RECOVERY_ON_TIMER_EXPIRE": "N",
    
    # Ocupado
    "USER_BUSY": "O",
    "NORMAL_CIRCUIT_CONGESTION": "O",
    
    # Fallida
    "CALL_REJECTED": "F",
    "USER_NOT_REGISTERED": "F",
    "UNALLOCATED_NUMBER": "F",
    "INVALID_NUMBER_FORMAT": "F",
    "NO_ROUTE_DESTINATION": "F",
    "DESTINATION_OUT_OF_ORDER": "F",
    "NETWORK_OUT_OF_ORDER": "F",
    "NORMAL_TEMPORARY_FAILURE": "F",
    
    # Máquina/Buzón
    "MACHINE_DETECTED": "M",
    
    # Abandonada (cliente colgó antes de transfer)
    "ORIGINATOR_CANCEL": "B",  # Si colgó mientras esperaba
}


@dataclass
class DialerStats:
    """Estadísticas del marcador predictivo"""
    campaign_name: str = ""
    
    # Contadores de llamadas
    calls_sent: int = 0
    calls_answered: int = 0
    calls_transferred: int = 0
    calls_completed: int = 0
    calls_no_answer: int = 0
    calls_busy: int = 0
    calls_failed: int = 0
    calls_machine: int = 0
    calls_abandoned: int = 0
    
    # Tracking de únicos
    unique_sent: Set = field(default_factory=set)
    unique_answered: Set = field(default_factory=set)
    unique_transferred: Set = field(default_factory=set)
    unique_completed: Set = field(default_factory=set)
    unique_no_answer: Set = field(default_factory=set)
    unique_busy: Set = field(default_factory=set)
    unique_failed: Set = field(default_factory=set)
    unique_machine: Set = field(default_factory=set)
    unique_abandoned: Set = field(default_factory=set)
    
    # Llamadas activas
    dialing_numbers: Set = field(default_factory=set)
    ringing_numbers: Set = field(default_factory=set)
    in_queue_numbers: Set = field(default_factory=set)
    with_agent_numbers: Set = field(default_factory=set)
    
    # Agentes
    total_agents: int = 0
    available_agents: int = 0
    busy_agents: int = 0
    
    # Métricas
    cps_current: float = 0.0
    cps_max: float = 0.0
    overdial_ratio: float = DIALER_OVERDIAL_RATIO
    abandon_rate: float = 0.0
    
    # Timing
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    duration: float = 0.0
    
    def calculate_abandon_rate(self) -> float:
        """Calcula el porcentaje de abandonos"""
        total_answered = len(self.unique_answered) + len(self.unique_abandoned)
        if total_answered > 0:
            self.abandon_rate = (len(self.unique_abandoned) / total_answered) * 100
        return self.abandon_rate
    
    def should_reduce_overdial(self) -> bool:
        """Determina si se debe reducir el sobrediscado"""
        return self.calculate_abandon_rate() > DIALER_ABANDON_THRESHOLD
    
    def to_dict(self) -> dict:
        """Convierte a diccionario para serialización"""
        if self.start_time:
            self.duration = time.time() - self.start_time
        
        duration_formatted = ""
        if self.duration > 0:
            minutes = int(self.duration // 60)
            seconds = int(self.duration % 60)
            duration_formatted = f"{minutes}m {seconds}s"
        
        total_processed = (len(self.unique_completed) + len(self.unique_no_answer) + 
                          len(self.unique_busy) + len(self.unique_failed) + 
                          len(self.unique_machine) + len(self.unique_abandoned))
        
        return {
            "campaign_name": self.campaign_name,
            "campaign_type": "Discador",
            
            # Llamadas
            "calls_sent": self.calls_sent,
            "calls_answered": len(self.unique_answered),
            "calls_transferred": len(self.unique_transferred),
            "calls_completed": len(self.unique_completed),
            "calls_no_answer": len(self.unique_no_answer),
            "calls_busy": len(self.unique_busy),
            "calls_failed": len(self.unique_failed),
            "calls_machine": len(self.unique_machine),
            "calls_abandoned": len(self.unique_abandoned),
            "total_processed": total_processed,
            
            # Activas
            "dialing": len(self.dialing_numbers),
            "ringing": len(self.ringing_numbers),
            "in_queue": len(self.in_queue_numbers),
            "with_agent": len(self.with_agent_numbers),
            "active_total": len(self.dialing_numbers) + len(self.ringing_numbers) + 
                           len(self.in_queue_numbers) + len(self.with_agent_numbers),
            
            # Agentes
            "total_agents": self.total_agents,
            "available_agents": self.available_agents,
            "busy_agents": self.busy_agents,
            
            # Métricas
            "cps": round(self.cps_current, 2),
            "cps_max": round(self.cps_max, 2),
            "overdial_ratio": round(self.overdial_ratio, 2),
            "abandon_rate": round(self.calculate_abandon_rate(), 2),
            
            # Duración
            "duration": round(self.duration, 2),
            "duration_formatted": duration_formatted,
        }
    
    def print_live(self):
        """Imprime estadísticas en tiempo real"""
        stats = self.to_dict()
        sys.stdout.write(
            f"\r📊 [{self.campaign_name}] "
            f"CPS: {stats['cps']:.1f} | "
            f"Enviadas: {stats['calls_sent']} | "
            f"EnCola: {stats['in_queue']} | "
            f"ConAgente: {stats['with_agent']} | "
            f"Completadas: {stats['calls_completed']} | "
            f"Abandonos: {stats['abandon_rate']:.1f}% | "
            f"Agentes: {stats['available_agents']}/{stats['total_agents']}\n"
        )
        sys.stdout.flush()


# =============================================================================
# FUNCIONES DE TRACKING
# =============================================================================

def register_uuid(uuid: str, campaign_name: str = None, numero: str = None):
    """Registra un UUID y número como activos"""
    with lock:
        ACTIVE_UUIDS.add(uuid)
        UUID_TIMESTAMPS[uuid] = time.time()
        if campaign_name and numero:
            key = (campaign_name, numero)
            ACTIVE_NUMBERS.add(key)
            NUMBER_TIMESTAMPS[key] = time.time()


def release_uuid(uuid: str, reason: str = "", campaign_name: str = None, numero: str = None):
    """Libera un UUID y número del tracking"""
    with lock:
        ACTIVE_UUIDS.discard(uuid)
        UUID_TIMESTAMPS.pop(uuid, None)
        if campaign_name and numero:
            key = (campaign_name, numero)
            ACTIVE_NUMBERS.discard(key)
            NUMBER_TIMESTAMPS.pop(key, None)
    logger.debug(f"🔓 UUID liberado: {uuid[:30]}... ({reason})")


def get_active_count() -> int:
    """Obtiene el número de UUIDs activos"""
    with lock:
        return len(ACTIVE_UUIDS)


def is_number_active(campaign_name: str, numero: str) -> bool:
    """Verifica si un número está actualmente en proceso"""
    with lock:
        key = (campaign_name, numero)
        if key in ACTIVE_NUMBERS:
            timestamp = NUMBER_TIMESTAMPS.get(key, 0)
            if time.time() - timestamp > 180:  # 3 minutos para discador
                ACTIVE_NUMBERS.discard(key)
                NUMBER_TIMESTAMPS.pop(key, None)
                return False
            return True
        return False


async def cleanup_stale():
    """Limpia UUIDs y números obsoletos"""
    current_time = time.time()
    stale_uuids = []
    stale_numbers = []
    
    with lock:
        for uuid, ts in list(UUID_TIMESTAMPS.items()):
            if current_time - ts > 180:  # 3 minutos
                stale_uuids.append(uuid)
        for key, ts in list(NUMBER_TIMESTAMPS.items()):
            if current_time - ts > 180:
                stale_numbers.append(key)
    
    for uuid in stale_uuids:
        with lock:
            ACTIVE_UUIDS.discard(uuid)
            UUID_TIMESTAMPS.pop(uuid, None)
    
    for key in stale_numbers:
        with lock:
            ACTIVE_NUMBERS.discard(key)
            NUMBER_TIMESTAMPS.pop(key, None)
    
    total = len(stale_uuids) + len(stale_numbers)
    if total > 0:
        logger.debug(f"🧹 Limpiados {total} elementos obsoletos")
    return total


# =============================================================================
# FUNCIONES DE CONFIGURACIÓN
# =============================================================================

def get_campaign_config(campaign_name: str) -> dict:
    """Obtiene configuración de campaña desde caché o BD"""
    current_time = time.time()
    
    if campaign_name in CAMPAIGN_CONFIG_CACHE:
        cache = CAMPAIGN_CONFIG_CACHE[campaign_name]
        if current_time - cache.get("timestamp", 0) < CACHE_TTL:
            return cache
    
    try:
        with engine.connect() as conn:
            # CPS se calcula automáticamente (máx 60 dividido entre campañas activas)
            # Usar queue_relationated en lugar de cola_destino
            # contexto no se usa - siempre es el nombre de la campaña
            result = conn.execute(text("""
                SELECT amd, horarios, activo, queue_relationated, 
                       max_concurrent, overdial_ratio
                FROM campanas 
                WHERE nombre = :nombre
            """), {"nombre": campaign_name}).fetchone()
            
            if result:
                config = {
                    "amd": result[0] or "PRO",
                    "horarios": result[1],
                    "activo": result[2],
                    "cola_destino": result[3] or DIALER_DEFAULT_QUEUE,
                    "cps": DIALER_CPS_GLOBAL,  # CPS automático
                    "max_concurrent": result[4] or DIALER_MAX_CONCURRENT_CALLS,
                    "overdial_ratio": float(result[5]) if result[5] else DIALER_OVERDIAL_RATIO,
                    "timestamp": current_time
                }
                CAMPAIGN_CONFIG_CACHE[campaign_name] = config
                return config
    except Exception as e:
        logger.debug(f"Error obteniendo config de {campaign_name}: {e}")
    
    return {
        "amd": "PRO",
        "horarios": None,
        "activo": "S",
        "cola_destino": DIALER_DEFAULT_QUEUE,
        "cps": DIALER_CPS_GLOBAL,
        "max_concurrent": DIALER_MAX_CONCURRENT_CALLS,
        "overdial_ratio": DIALER_OVERDIAL_RATIO,
        "timestamp": current_time
    }


def is_in_schedule(horario_str: str) -> bool:
    """Verifica si el horario actual está dentro del rango permitido"""
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


# =============================================================================
# FUNCIONES DE AGENTES
# =============================================================================

async def get_available_agents(campaign_name: str, cola: str) -> dict:
    """
    Obtiene el número de agentes disponibles para una cola.
    Consulta FreeSWITCH vía ESL o usa la tabla de agentes en BD.
    """
    current_time = time.time()
    
    # Verificar caché
    cache_key = f"{campaign_name}:{cola}"
    if cache_key in AGENTS_CACHE:
        cached = AGENTS_CACHE[cache_key]
        if current_time - cached.get("last_update", 0) < 5:  # 5 segundos de caché
            return cached
    
    # Consultar BD para obtener estado de agentes
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN estado = 'disponible' THEN 1 ELSE 0 END) as disponibles,
                    SUM(CASE WHEN estado = 'en_llamada' THEN 1 ELSE 0 END) as ocupados,
                    SUM(CASE WHEN estado = 'pausa' THEN 1 ELSE 0 END) as pausados
                FROM agentes
                WHERE cola = :cola AND activo = 'S'
            """), {"cola": cola}).fetchone()
            
            if result:
                agents_info = {
                    "total": result[0] or 0,
                    "available": result[1] or 0,
                    "busy": result[2] or 0,
                    "paused": result[3] or 0,
                    "last_update": current_time
                }
                AGENTS_CACHE[cache_key] = agents_info
                return agents_info
    except Exception as e:
        # Si no existe la tabla agentes, simular con un valor por defecto
        logger.debug(f"No se pudo obtener agentes de BD: {e}")
    
    # Valor por defecto si no hay tabla de agentes
    default_agents = {
        "total": 5,
        "available": 3,
        "busy": 2,
        "paused": 0,
        "last_update": current_time
    }
    AGENTS_CACHE[cache_key] = default_agents
    return default_agents


async def get_queue_status_from_fs(con, queue_extension: str) -> dict:
    """
    Consulta el estado de una cola de FreeSWITCH usando ESL.
    Usa el comando fifo list o callcenter_config agent list.
    """
    try:
        loop = asyncio.get_event_loop()
        
        # Intentar obtener estado de fifo
        response = await loop.run_in_executor(
            ESL_EXECUTOR,
            lambda: con.api(f"fifo list {queue_extension}")
        )
        
        if response:
            body = response.getBody()
            # Parsear respuesta para extraer agentes
            # El formato depende de tu configuración de FreeSWITCH
            logger.debug(f"FIFO status: {body[:200] if body else 'empty'}")
            
        # También intentar callcenter
        response_cc = await loop.run_in_executor(
            ESL_EXECUTOR,
            lambda: con.api("callcenter_config agent list")
        )
        
        if response_cc:
            body = response_cc.getBody()
            logger.debug(f"Callcenter agents: {body[:200] if body else 'empty'}")
        
    except Exception as e:
        logger.debug(f"Error consultando estado de cola: {e}")
    
    return {"available": 0, "busy": 0, "total": 0}


# =============================================================================
# FUNCIONES DE WEBSOCKET
# =============================================================================

async def safe_send_ws(func, *args, **kwargs):
    """Envío seguro al WebSocket"""
    if not WEBSOCKET_AVAILABLE:
        return
    try:
        await func(*args, **kwargs)
    except Exception as e:
        logger.warning(f"⚠️ Error WebSocket: {e}")


# =============================================================================
# FUNCIONES DE ENVÍO DE LLAMADAS
# =============================================================================

async def send_originate_batch(con, batch_data: list, campaign_name: str) -> list:
    """
    Envía múltiples originates con control de throttle.
    Devuelve lista de (numero, uuid, success)
    """
    loop = asyncio.get_event_loop()
    results = []
    
    async def send_single(numero, uuid, originate_str):
        try:
            response = await loop.run_in_executor(
                ESL_EXECUTOR,
                lambda: con.api(originate_str)
            )
            
            if response:
                body = response.getBody()
                success = "+OK" in body or "Job-UUID" in body
                
                if success:
                    logger.debug(f"✅ [{campaign_name}] Originate OK: {numero}")
                else:
                    logger.warning(f"⚠️ [{campaign_name}] Originate fallo: {numero} - {body[:100]}")
                
                return (numero, uuid, success)
            return (numero, uuid, False)
        except Exception as e:
            logger.error(f"❌ Error originate {numero}: {e}")
            return (numero, uuid, False)
    
    # Enviar en mini-batches para evitar throttle
    MINI_BATCH = 5
    DELAY = 0.35
    
    tasks = [send_single(n, u, o) for n, u, o in batch_data]
    
    for i in range(0, len(tasks), MINI_BATCH):
        batch_tasks = tasks[i:i+MINI_BATCH]
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        for r in batch_results:
            if isinstance(r, Exception):
                logger.error(f"Error en batch: {r}")
            else:
                results.append(r)
        
        if i + MINI_BATCH < len(tasks):
            await asyncio.sleep(DELAY)
    
    return results


async def send_calls_for_dialer_campaign(
    campaign_name: str, 
    numbers: list, 
    cps: int,
    max_intentos: int,
    stats: DialerStats = None
) -> DialerStats:
    """
    Envía llamadas para una campaña de tipo Discador.
    Similar a send_calls_for_campaign pero con lógica de sobrediscado y transferencia a cola.
    """
    stats = stats or DialerStats(campaign_name=campaign_name)
    stats.campaign_name = campaign_name
    stats.start_time = time.time()
    
    log = logger.info
    
    log(f"🚦 [{campaign_name}] ===== INICIO DISCADOR =====")
    
    # Validar números
    valid_numbers = [n for n in numbers if n.strip().isdigit()]
    log(f"📦 [{campaign_name}] {len(valid_numbers)} números válidos")
    
    if not valid_numbers:
        log(f"📋 [{campaign_name}] Sin números pendientes")
        return stats
    
    # Obtener configuración
    config = get_campaign_config(campaign_name)
    cola_destino = config.get("cola_destino", DIALER_DEFAULT_QUEUE)
    amd_type = config.get("amd", "PRO")
    stats.overdial_ratio = config.get("overdial_ratio", DIALER_OVERDIAL_RATIO)
    
    log(f"📞 [{campaign_name}] Cola AMI: {cola_destino} | IVR: {campaign_name}")
    log(f"📊 [{campaign_name}] CPS: {cps} | Overdial: {stats.overdial_ratio}")
    
    # Conexión ESL
    con = ESL.ESLconnection(FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD)
    if not con.connected():
        log(f"❌ [{campaign_name}] No se pudo conectar a FreeSWITCH ESL")
        return stats
    
    log(f"✅ [{campaign_name}] Conectado a FreeSWITCH ESL")
    
    # Guardar timestamp de inicio
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE campanas SET fecha_inicio = NOW() WHERE nombre = :nombre
            """), {"nombre": campaign_name})
    except Exception as e:
        logger.warning(f"⚠️ Error guardando timestamp: {e}")
    
    total = len(valid_numbers)
    i = 0
    delay = 1 / cps
    
    # Bucle principal
    while i < total:
        # Verificar si campaña sigue activa
        try:
            with engine.connect() as conn:
                status = conn.execute(
                    text("SELECT activo FROM campanas WHERE nombre = :nombre"),
                    {"nombre": campaign_name}
                ).fetchone()
                
                if not status or status[0] != 'S':
                    log(f"⏸️ [{campaign_name}] Campaña no activa - deteniendo")
                    break
        except Exception as e:
            logger.error(f"Error verificando estado: {e}")
            break
        
        # Verificar horario
        config = get_campaign_config(campaign_name)
        if not is_in_schedule(config.get("horarios")):
            log(f"⏸️ [{campaign_name}] Fuera de horario - pausando")
            try:
                with engine.begin() as conn:
                    conn.execute(
                        text("UPDATE campanas SET activo = 'P' WHERE nombre = :nombre"),
                        {"nombre": campaign_name}
                    )
            except:
                pass
            break
        
        # Obtener agentes disponibles
        agents = await get_available_agents(campaign_name, cola_destino)
        stats.total_agents = agents.get("total", 0)
        stats.available_agents = agents.get("available", 0)
        stats.busy_agents = agents.get("busy", 0)
        
        if stats.available_agents < DIALER_MIN_AGENTS:
            log(f"⏳ [{campaign_name}] Esperando agentes ({stats.available_agents} disponibles)")
            await asyncio.sleep(2)
            continue
        
        # Ajustar overdial si hay muchos abandonos
        if stats.should_reduce_overdial():
            stats.overdial_ratio = max(1.0, stats.overdial_ratio - 0.1)
            log(f"📉 [{campaign_name}] Reduciendo overdial a {stats.overdial_ratio:.2f} (abandonos: {stats.abandon_rate:.1f}%)")
        
        # Calcular batch size basado en agentes y overdial
        batch_size = int(stats.available_agents * stats.overdial_ratio)
        batch_size = max(1, min(batch_size, cps, total - i))
        
        # Limitar por concurrencia
        current_active = get_active_count()
        if current_active >= DIALER_MAX_CONCURRENT_CALLS:
            log(f"⏳ [{campaign_name}] Max concurrencia ({current_active})")
            await asyncio.sleep(1)
            continue
        
        batch_size = min(batch_size, DIALER_MAX_CONCURRENT_CALLS - current_active)
        batch = valid_numbers[i:i + batch_size]
        
        log(f"📞 [{campaign_name}] Discando {len(batch)} ({i + len(batch)}/{total}) | Agentes: {stats.available_agents}")
        
        # Preparar originates
        batch_originates = []
        skipped = 0
        
        for numero in batch:
            # Verificar si ya está activo
            if is_number_active(campaign_name, numero):
                skipped += 1
                continue
            
            # Verificar estado en BD
            try:
                with engine.connect() as conn:
                    check = conn.execute(text(f"""
                        SELECT estado FROM `{campaign_name}`
                        WHERE telefono = :numero AND estado IN ('P', 'R', 'A', 'Q', 'T')
                        LIMIT 1
                    """), {"numero": numero}).fetchone()
                    
                    if check:
                        skipped += 1
                        continue
            except:
                pass
            
            uuid = f"dialer_{campaign_name}_{numero}_{int(time.time()*1000000)}"
            
            # Registrar como activo
            register_uuid(uuid, campaign_name, numero)
            
            # Actualizar BD a estado P (procesando/discando)
            try:
                with engine.begin() as conn:
                    result = conn.execute(text(f"""
                        UPDATE `{campaign_name}` 
                        SET uuid = :uuid, 
                            estado = 'P',
                            fecha_envio = NOW(),
                            intentos = COALESCE(intentos, 0) + 1
                        WHERE telefono = :numero
                        AND estado IN ('pendiente', 'N', 'O', 'F', 'B', 'E', 'X')
                    """), {"uuid": uuid, "numero": numero})
                    
                    if result.rowcount == 0:
                        release_uuid(uuid, "already_processed", campaign_name, numero)
                        continue
            except Exception as e:
                logger.warning(f"⚠️ Error actualizando {numero}: {e}")
                release_uuid(uuid, "update_failed", campaign_name, numero)
                continue
            
            # Construir originate con transfer a cola
            # El dialplan debe tener la lógica de:
            # 1. AMD si está configurado
            # 2. Si contesta humano, transfer a la cola de agentes
            #
            # Ejemplo de originate:
            # - ignore_early_media=false para detectar cuando contesta
            # - execute_on_answer transfiere a la cola
            
            # El transfer va al IVR de la campaña (dialplan con nombre de campaña)
            # El IVR decide a qué cola transferir después
            # cola_destino (queue_relationated) es solo para monitoreo AMI
            if amd_type and amd_type.upper() == "PRO":
                # Con AMD: primero detecta, luego transfiere al IVR
                originate_str = (
                    f"bgapi originate "
                    f"{{ignore_early_media=false,"
                    f"origination_uuid={uuid},"
                    f"campaign_name='{campaign_name}',"
                    f"campaign_type='Discador',"
                    f"dialer_queue='{cola_destino}',"
                    f"origination_caller_id_number='{numero}',"
                    f"execute_on_answer='transfer 9999 XML {campaign_name}'}}"
                    f"sofia/gateway/{GATEWAY}/{numero} 2222 XML DETECT_AMD_DIALER"
                )
            else:
                # Sin AMD: transfiere directo al IVR cuando contesta
                originate_str = (
                    f"bgapi originate "
                    f"{{ignore_early_media=false,"
                    f"origination_uuid={uuid},"
                    f"campaign_name='{campaign_name}',"
                    f"campaign_type='Discador',"
                    f"dialer_queue='{cola_destino}',"
                    f"origination_caller_id_number='{numero}',"
                    f"execute_on_answer='transfer 9999 XML {campaign_name}'}}"
                    f"sofia/gateway/{GATEWAY}/{numero} &park()"
                )
            
            batch_originates.append((numero, uuid, originate_str))
            stats.dialing_numbers.add(numero)
        
        if skipped > 0:
            log(f"🚫 [{campaign_name}] Saltados {skipped} números")
        
        if not batch_originates:
            i += batch_size
            continue
        
        # Enviar batch
        results = await send_originate_batch(con, batch_originates, campaign_name)
        
        # Procesar resultados
        success_count = 0
        for numero, uuid, success in results:
            if not success:
                release_uuid(uuid, "originate_failed", campaign_name, numero)
                stats.dialing_numbers.discard(numero)
                try:
                    with engine.begin() as conn:
                        conn.execute(text(f"""
                            UPDATE `{campaign_name}` 
                            SET estado = 'F', hangup_cause = 'ORIGINATE_FAILED'
                            WHERE uuid = :uuid
                        """), {"uuid": uuid})
                except:
                    pass
                continue
            
            success_count += 1
            stats.calls_sent += 1
            stats.unique_sent.add(numero)
            
            # WebSocket
            asyncio.create_task(safe_send_ws(send_event_to_websocket, "dialer_call_sent", {
                "campaign": campaign_name,
                "numero": numero,
                "uuid": uuid,
                "status": "dialing",
                "stats": stats.to_dict()
            }))
        
        # Registrar en Redis para CPS
        if REDIS_AVAILABLE and success_count > 0 and redis_client:
            try:
                current_ts = time.time()
                pipe = redis_client.pipeline()
                key = f"dialer:{campaign_name}:cps"
                for j in range(success_count):
                    pipe.zadd(key, {str(current_ts + j*0.001): current_ts + j*0.001})
                pipe.zremrangebyscore(key, 0, current_ts - 60)
                pipe.execute()
            except:
                pass
        
        log(f"✅ [{campaign_name}] Enviadas: {success_count}/{len(batch)}")
        
        # Delay
        elapsed = time.time() - stats.start_time
        remaining = max(0.3, 1.5 - (time.time() % 1.5))
        await asyncio.sleep(remaining)
        
        i += batch_size
        
        # Limpieza periódica
        if i % 30 == 0:
            await cleanup_stale()
    
    # Cerrar conexión
    try:
        if con and con.connected():
            con.disconnect()
    except:
        pass
    
    # Estadísticas finales
    stats.end_time = time.time()
    stats.duration = stats.end_time - stats.start_time
    
    # Guardar en BD
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE campanas 
                SET duracion_total = :duracion, fecha_fin = NOW()
                WHERE nombre = :nombre
            """), {"duracion": int(stats.duration), "nombre": campaign_name})
    except:
        pass
    
    log(f"")
    log(f"{'='*70}")
    log(f"✅ [{campaign_name}] DISCADOR FINALIZADO")
    log(f"   📞 Enviadas: {stats.calls_sent}")
    log(f"   ✅ Completadas: {len(stats.unique_completed)}")
    log(f"   ❌ Abandonos: {len(stats.unique_abandoned)} ({stats.abandon_rate:.1f}%)")
    log(f"   ⏱️  Duración: {stats.duration:.2f}s")
    log(f"{'='*70}")
    
    return stats


# =============================================================================
# EVENT LISTENER
# =============================================================================

async def esl_event_listener():
    """Listener de eventos ESL para el discador"""
    global EVENT_LISTENER_RUNNING
    EVENT_LISTENER_RUNNING = True
    
    logger.info("🎧 Iniciando ESL Event Listener para Discador...")
    
    loop = asyncio.get_event_loop()
    reconnect_delay = 1
    
    while EVENT_LISTENER_RUNNING:
        con = None
        try:
            con = ESL.ESLconnection(FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD)
            
            if not con.connected():
                logger.warning(f"⚠️ ESL Listener: reconectando en {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)
                continue
            
            logger.info("✅ ESL Event Listener conectado")
            reconnect_delay = 1
            
            # Suscribirse a eventos
            con.events("plain", "CHANNEL_HANGUP CHANNEL_HANGUP_COMPLETE CHANNEL_ANSWER CHANNEL_BRIDGE")
            logger.info("📡 Suscrito a eventos: HANGUP, ANSWER, BRIDGE")
            
            while EVENT_LISTENER_RUNNING and con.connected():
                event = await loop.run_in_executor(
                    ESL_EXECUTOR,
                    lambda: con.recvEventTimed(1000)
                )
                
                if not event:
                    continue
                
                event_name = event.getHeader("Event-Name")
                
                if event_name in ["CHANNEL_HANGUP", "CHANNEL_HANGUP_COMPLETE"]:
                    await handle_hangup_event(event)
                elif event_name == "CHANNEL_ANSWER":
                    await handle_answer_event(event)
                elif event_name == "CHANNEL_BRIDGE":
                    await handle_bridge_event(event)
            
            logger.warning("⚠️ ESL Listener: conexión perdida")
            
        except Exception as e:
            logger.error(f"❌ Error en ESL Listener: {e}")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 30)
        finally:
            if con:
                try:
                    con.disconnect()
                except:
                    pass
    
    logger.info("🛑 ESL Event Listener detenido")


async def handle_hangup_event(event):
    """Maneja eventos de hangup"""
    uuid = event.getHeader("Unique-ID") or event.getHeader("variable_origination_uuid")
    hangup_cause = event.getHeader("Hangup-Cause") or "UNKNOWN"
    campaign = event.getHeader("variable_campaign_name")
    campaign_type = event.getHeader("variable_campaign_type")
    numero = event.getHeader("Caller-Caller-ID-Number") or event.getHeader("variable_origination_caller_id_number")
    
    # Solo procesar si es una llamada de discador
    if campaign_type != "Discador":
        return
    
    if not uuid or not campaign:
        return
    
    # Verificar si está siendo trackeado
    with lock:
        if uuid not in ACTIVE_UUIDS:
            return
    
    # Determinar nuevo estado
    new_state = HANGUP_CAUSE_TO_DIALER_STATE.get(hangup_cause, "F")
    
    logger.info(f"📴 [{campaign}] Hangup: {numero} | {hangup_cause} -> {new_state}")
    
    # Actualizar BD
    try:
        with engine.begin() as conn:
            conn.execute(text(f"""
                UPDATE `{campaign}`
                SET estado = :estado,
                    hangup_cause = :cause,
                    fecha_fin = NOW()
                WHERE uuid = :uuid
                AND estado NOT IN ('C')
            """), {"estado": new_state, "cause": hangup_cause, "uuid": uuid})
    except Exception as e:
        logger.error(f"Error actualizando hangup: {e}")
    
    # Liberar UUID
    release_uuid(uuid, f"hangup_{hangup_cause}", campaign, numero)
    
    # WebSocket
    await safe_send_ws(send_event_to_websocket, "dialer_call_ended", {
        "campaign": campaign,
        "numero": numero,
        "uuid": uuid,
        "state": new_state,
        "hangup_cause": hangup_cause
    })


async def handle_answer_event(event):
    """Maneja eventos de llamada contestada"""
    uuid = event.getHeader("Unique-ID")
    campaign = event.getHeader("variable_campaign_name")
    campaign_type = event.getHeader("variable_campaign_type")
    numero = event.getHeader("variable_origination_caller_id_number")
    
    if campaign_type != "Discador":
        return
    
    if not uuid or not campaign:
        return
    
    logger.info(f"📞 [{campaign}] Contestada: {numero}")
    
    # Actualizar BD
    try:
        with engine.begin() as conn:
            conn.execute(text(f"""
                UPDATE `{campaign}`
                SET estado = 'A',
                    fecha_respuesta = NOW()
                WHERE uuid = :uuid
            """), {"uuid": uuid})
    except Exception as e:
        logger.error(f"Error actualizando answer: {e}")
    
    # WebSocket
    await safe_send_ws(send_event_to_websocket, "dialer_call_answered", {
        "campaign": campaign,
        "numero": numero,
        "uuid": uuid
    })


async def handle_bridge_event(event):
    """Maneja eventos de bridge (transferido a agente)"""
    uuid = event.getHeader("Unique-ID")
    campaign = event.getHeader("variable_campaign_name")
    campaign_type = event.getHeader("variable_campaign_type")
    numero = event.getHeader("variable_origination_caller_id_number")
    other_leg = event.getHeader("Bridge-B-Unique-ID")
    
    if campaign_type != "Discador":
        return
    
    if not uuid or not campaign:
        return
    
    logger.info(f"🎧 [{campaign}] Transferida a agente: {numero}")
    
    # Actualizar BD
    try:
        with engine.begin() as conn:
            conn.execute(text(f"""
                UPDATE `{campaign}`
                SET estado = 'T',
                    fecha_transfer = NOW()
                WHERE uuid = :uuid
            """), {"uuid": uuid})
    except Exception as e:
        logger.error(f"Error actualizando transfer: {e}")
    
    # WebSocket
    await safe_send_ws(send_event_to_websocket, "dialer_call_transferred", {
        "campaign": campaign,
        "numero": numero,
        "uuid": uuid
    })


# =============================================================================
# WORKERS Y MONITOR
# =============================================================================

async def dialer_worker(worker_id: int, queue: asyncio.Queue):
    """Worker que procesa campañas de discador"""
    logger.info(f"🔧 Dialer Worker #{worker_id} iniciado")
    
    while True:
        try:
            try:
                data = await asyncio.wait_for(queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                continue
            
            if data is None:
                break
            
            campaign_name = data['campaign_name']
            numbers = data['numbers']
            cps = data['cps']
            max_intentos = data['max_intentos']
            active_ref = data.get('active_ref')
            
            logger.info(f"🚀 Worker #{worker_id}: {campaign_name} ({len(numbers)} números)")
            
            try:
                await send_calls_for_dialer_campaign(
                    campaign_name=campaign_name,
                    numbers=numbers,
                    cps=cps,
                    max_intentos=max_intentos
                )
            except Exception as e:
                logger.error(f"❌ Error en worker: {e}")
                logger.error(traceback.format_exc())
            finally:
                if active_ref and campaign_name in active_ref:
                    active_ref.discard(campaign_name)
            
            queue.task_done()
            
        except Exception as e:
            logger.error(f"❌ Error en worker #{worker_id}: {e}")
            await asyncio.sleep(1)


async def get_pending_numbers_dialer(campaign_name: str, max_intentos: int) -> list:
    """Obtiene números pendientes para campaña de discador"""
    try:
        with engine.connect() as conn:
            query = text(f"""
                SELECT DISTINCT telefono FROM `{campaign_name}`
                WHERE (
                    estado = 'pendiente'
                    OR (estado IN ('N', 'O', 'F', 'B', 'E', 'X') 
                        AND (intentos IS NULL OR intentos < :max_intentos))
                )
                AND estado NOT IN ('C', 'T', 'P', 'R', 'A', 'Q', 'M')
            """)
            result = conn.execute(query, {"max_intentos": max_intentos})
            
            numbers = []
            for row in result:
                num = row[0]
                if not is_number_active(campaign_name, num):
                    numbers.append(num)
            
            return numbers
    except Exception as e:
        logger.error(f"Error obteniendo pendientes: {e}")
        return []


async def monitor_dialer_campaigns(queue: asyncio.Queue):
    """Monitorea campañas de tipo Discador"""
    logger.info("🔍 Iniciando monitor de campañas Discador...")
    
    active_tasks = set()
    
    while True:
        try:
            with engine.connect() as conn:
                # Campañas activas de tipo Discador
                campaigns = conn.execute(text("""
                    SELECT nombre, reintentos, horarios
                    FROM campanas
                    WHERE activo = 'S'
                    AND tipo = 'Discador'
                    AND (fecha_programada IS NULL OR fecha_programada <= NOW())
                """)).fetchall()
                
                # Reactivar pausadas que entran en horario
                paused = conn.execute(text("""
                    SELECT nombre, reintentos, horarios
                    FROM campanas
                    WHERE activo = 'P'
                    AND tipo = 'Discador'
                """)).fetchall()
                
                for row in paused:
                    nombre, _, horario = row[0], row[1], row[2] if len(row) > 2 else None
                    if is_in_schedule(horario):
                        logger.info(f"🔄 Reactivando {nombre}")
                        with engine.begin() as c:
                            c.execute(
                                text("UPDATE campanas SET activo = 'S' WHERE nombre = :n"),
                                {"n": nombre}
                            )
            
            # Procesar campañas
            for row in campaigns:
                nombre = row[0]
                reintentos = row[1] or 3
                horario = row[2] if len(row) > 2 else None
                
                if nombre in active_tasks:
                    continue
                
                # Verificar tabla existe
                try:
                    with engine.connect() as conn:
                        conn.execute(text(f"SELECT 1 FROM `{nombre}` LIMIT 1"))
                except:
                    continue
                
                # Verificar horario
                if not is_in_schedule(horario):
                    continue
                
                # Obtener pendientes
                numbers = await get_pending_numbers_dialer(nombre, reintentos)
                
                if numbers:
                    config = get_campaign_config(nombre)
                    cps = config.get("cps", DIALER_CPS_GLOBAL)
                    
                    active_tasks.add(nombre)
                    
                    await queue.put({
                        'campaign_name': nombre,
                        'numbers': numbers,
                        'cps': cps,
                        'max_intentos': reintentos,
                        'active_ref': active_tasks
                    })
                    
                    logger.info(f"📋 [{nombre}] Encolada: {len(numbers)} números")
                else:
                    # Verificar si debe finalizarse
                    try:
                        with engine.connect() as conn:
                            stats = conn.execute(text(f"""
                                SELECT
                                    COUNT(CASE WHEN estado IN ('P','R','A','Q','T') THEN 1 END),
                                    COUNT(*),
                                    COUNT(CASE WHEN estado IN ('C','N','O','F','M','B','X','E') THEN 1 END)
                                FROM `{nombre}`
                            """)).fetchone()
                            
                            if stats[0] == 0 and stats[2] >= stats[1] and stats[1] > 0:
                                logger.info(f"🏁 Finalizando {nombre}")
                                with engine.begin() as c:
                                    c.execute(
                                        text("UPDATE campanas SET activo = 'F' WHERE nombre = :n"),
                                        {"n": nombre}
                                    )
                    except:
                        pass
            
            await asyncio.sleep(5)
            
        except Exception as e:
            logger.error(f"❌ Error en monitor: {e}")
            await asyncio.sleep(5)


async def periodic_cleanup():
    """Limpieza periódica"""
    while True:
        try:
            await asyncio.sleep(30)
            await cleanup_stale()
        except Exception as e:
            logger.error(f"Error en limpieza: {e}")


# =============================================================================
# MAIN
# =============================================================================

async def main():
    """Función principal"""
    logger.info("=" * 60)
    logger.info("📞 DIALER SENDER - Marcador Predictivo")
    logger.info("=" * 60)
    
    logger.info("")
    logger.info("📡 Configuración:")
    logger.info(f"   FreeSWITCH: {FREESWITCH_HOST}:{FREESWITCH_PORT}")
    logger.info(f"   Gateway: {GATEWAY}")
    logger.info(f"   Max Concurrent: {DIALER_MAX_CONCURRENT_CALLS}")
    logger.info(f"   CPS: {DIALER_CPS_GLOBAL}")
    logger.info(f"   Overdial: {DIALER_OVERDIAL_RATIO}")
    logger.info(f"   Cola Default: {DIALER_DEFAULT_QUEUE}")
    if REDIS_AVAILABLE:
        logger.info(f"   Redis: ✅ Conectado")
    else:
        logger.info(f"   Redis: ❌ No disponible")
    logger.info("")
    
    # Cola de campañas
    campaign_queue = asyncio.Queue(maxsize=50)
    
    # Iniciar workers
    logger.info(f"🔧 Iniciando {DIALER_SENDER_WORKERS} workers...")
    workers = []
    for i in range(DIALER_SENDER_WORKERS):
        worker = asyncio.create_task(dialer_worker(i, campaign_queue))
        workers.append(worker)
    
    # Monitor de campañas
    monitor_task = asyncio.create_task(monitor_dialer_campaigns(campaign_queue))
    
    # Limpieza periódica
    cleanup_task = asyncio.create_task(periodic_cleanup())
    
    # Event listener
    global EVENT_LISTENER_TASK
    EVENT_LISTENER_TASK = asyncio.create_task(esl_event_listener())
    
    logger.info("")
    logger.info("🎯 Sistema listo - Monitoreando campañas Discador...")
    logger.info("💡 Presiona Ctrl+C para detener")
    logger.info("")
    
    try:
        await asyncio.gather(monitor_task, cleanup_task, EVENT_LISTENER_TASK, *workers)
    except KeyboardInterrupt:
        logger.info("\n⏹️ Deteniendo...")
    finally:
        global EVENT_LISTENER_RUNNING
        EVENT_LISTENER_RUNNING = False
        
        for _ in range(DIALER_SENDER_WORKERS):
            await campaign_queue.put(None)
        await asyncio.gather(*workers, return_exceptions=True)
        logger.info("✅ Workers detenidos")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️ Dialer Sender detenido")
    except Exception as e:
        logger.error(f"❌ Error fatal: {e}")
        sys.exit(1)
