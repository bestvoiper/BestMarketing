"""
State Updater - Proceso independiente para obtener estados ESL y actualizar Redis/MySQL
Este proceso corre de forma independiente con múltiples workers para máxima velocidad.
Funciona con REDIS + MYSQL para máximo rendimiento y persistencia.
"""
import asyncio
import time
import json
import sys
import math
import redis
from datetime import datetime
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor
from collections import deque
import threading

# Importar configuración compartida
from shared_config import (
    FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD,
    DB_URL, TERMINAL_STATES, STATE_UPDATE_WORKERS,
    REDIS_HOST, REDIS_PORT, REDIS_DB,
    get_logger, create_db_engine, is_terminal_state
)

logger = get_logger("state_updater")

# Importar ESL
try:
    import ESL
except ImportError:
    print("Debes instalar los bindings oficiales de FreeSWITCH ESL para Python")
    exit(1)

# Integración con Redis - Conexión directa para máxima velocidad
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
    # Test de conexión
    redis_client.ping()
    REDIS_AVAILABLE = True
    logger.info("✅ Redis conectado directamente (alta velocidad)")
    
    # También cargar RedisCallManager para funciones avanzadas
    try:
        from redis_manager import RedisCallManager
        redis_manager = RedisCallManager(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        logger.info("✅ RedisCallManager cargado para funciones avanzadas")
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
ws_server_instance = None
try:
    from websocket_server import send_stats_to_websocket, send_event_to_websocket, ws_server
    ws_server_instance = ws_server
    WEBSOCKET_AVAILABLE = True
    logger.info("✅ WebSocket disponible - estadísticas en vivo activadas")
except ImportError:
    WEBSOCKET_AVAILABLE = False
    logger.info("⚠️ WebSocket no disponible - funcionando sin estadísticas en vivo")

# Pool de conexiones
engine = create_db_engine(pool_size=50, max_overflow=25)

# ThreadPoolExecutor para operaciones paralelas
DB_EXECUTOR = ThreadPoolExecutor(max_workers=20, thread_name_prefix="db_worker")

# Cola de eventos para procesar
EVENT_QUEUE = None

# Tracking de UUIDs activos (compartido vía Redis)
GLOBAL_ACTIVE_UUIDS = set()
GLOBAL_UUID_TIMESTAMPS = {}

# Tracking de tiempos de respuesta
answered_times = {}

# Lock para operaciones thread-safe
state_lock = threading.Lock()


async def safe_send_to_websocket(func, *args, **kwargs):
    """Envío seguro al WebSocket - inicia servidor automáticamente si no está corriendo"""
    if not WEBSOCKET_AVAILABLE:
        return
    try:
        await func(*args, **kwargs)
    except OSError as e:
        if hasattr(e, 'errno') and e.errno == 98:
            logger.warning("⚠️ El WebSocket ya está corriendo en el puerto 8765. Solo debe haber un proceso escuchando en ese puerto.")
        else:
            logger.warning(f"⚠️ Error enviando al WebSocket: {e}")
    except Exception as e:
        logger.warning(f"⚠️ Error enviando al WebSocket: {e}")


def get_campaign_stats_fast(campaign_name: str) -> dict:
    """
    Obtiene estadísticas rápidas de una campaña para enviar al WebSocket.
    Usa Redis si está disponible, si no, consulta MySQL.
    """
    try:
        stats = {
            "campaign_name": campaign_name,
            "calls_sent": 0,
            "calls_by_state": {},
            "cps": 0.0,
            "cps_max": 0.0,
            "duration": 0.0,
            "duration_formatted": ""
        }
        
        # Intentar obtener de Redis primero (más rápido)
        if REDIS_AVAILABLE and redis_manager:
            try:
                redis_stats = redis_manager.get_campaign_stats(campaign_name)
                if redis_stats:
                    stats.update({
                        "calls_by_state": redis_stats.get("calls_by_state", {}),
                        "cps": redis_stats.get("cps", 0.0),
                        "cps_max": redis_stats.get("cps_max", 0.0),
                    })
                    # Calcular calls_sent (todo excepto pendiente)
                    for estado, count in stats["calls_by_state"].items():
                        if estado != 'pendiente':
                            stats["calls_sent"] += count
                    return stats
            except Exception as e:
                logger.debug(f"Error obteniendo stats de Redis: {e}")
        
        # Fallback a MySQL
        with engine.connect() as conn:
            result = conn.execute(
                text(f"""
                    SELECT estado, COUNT(*) as count
                    FROM `{campaign_name}`
                    GROUP BY estado
                """)
            )
            for row in result:
                estado = row[0] or 'unknown'
                count = row[1] or 0
                stats["calls_by_state"][estado] = count
                if estado != 'pendiente':
                    stats["calls_sent"] += count
        
        return stats
        
    except Exception as e:
        logger.debug(f"Error obteniendo stats rápidas: {e}")
        return {
            "campaign_name": campaign_name,
            "calls_sent": 0,
            "calls_by_state": {},
            "cps": 0.0
        }


def extract_field(event_str, field):
    """Extrae un campo de un evento ESL"""
    try:
        return event_str.split(f"{field}: ")[1].split("\n")[0].strip()
    except:
        return ""


def update_call_status_sync(campaign_name: str, numero: str, estado: str, 
                            duracion: str = "0", uuid: str = None, 
                            hangup_reason: str = None, amd_result: str = None,
                            incrementar_intento: bool = False):
    """
    Actualiza el estado de una llamada en MySQL y Redis (SÍNCRONO)
    Diseñado para ser ejecutado en ThreadPoolExecutor
    REDIS + MYSQL para máximo rendimiento y persistencia
    """
    try:
        # ===== ACTUALIZAR MYSQL =====
        with engine.begin() as conn:
            # Construir query UPDATE dinámicamente
            update_fields = ["estado = :estado", "duracion = :duracion"]
            params = {"estado": estado, "duracion": duracion, "numero": numero}
            
            if uuid:
                update_fields.append("uuid = :uuid")
                params["uuid"] = uuid
            
            if hangup_reason:
                update_fields.append("hangup_reason = :hangup_reason")
                params["hangup_reason"] = hangup_reason
            
            if amd_result:
                update_fields.append("amd_result = :amd_result")
                params["amd_result"] = amd_result
            
            if incrementar_intento:
                update_fields.append("intentos = intentos + 1")
            
            # Actualizar fecha_envio si no es pendiente
            if estado != 'pendiente':
                update_fields.append("fecha_envio = NOW()")
            
            query = f"UPDATE `{campaign_name}` SET {', '.join(update_fields)} WHERE telefono = :numero"
            result = conn.execute(text(query), params)
            logger.debug(f"✅ MySQL: {campaign_name} - {numero} -> {estado} (rows: {result.rowcount})")
        
        # ===== ACTUALIZAR REDIS =====
        if REDIS_AVAILABLE and redis_client:
            try:
                # Crear datos de la llamada
                call_data = {
                    "numero": numero,
                    "uuid": uuid or "",
                    "estado": estado,
                    "duracion": duracion,
                    "timestamp": datetime.now().isoformat(),
                    "campaign_name": campaign_name
                }
                
                if hangup_reason:
                    call_data["hangup_reason"] = hangup_reason
                if amd_result:
                    call_data["amd_result"] = amd_result
                
                # Pipeline para operaciones atómicas y rápidas
                pipe = redis_client.pipeline()
                
                # Guardar estado por UUID
                if uuid:
                    campaign_key = f"campaign:{campaign_name}:calls"
                    pipe.hset(campaign_key, uuid, json.dumps(call_data))
                    
                    # Guardar en hash global
                    pipe.hset("global:active_calls", uuid, json.dumps(call_data))
                
                # Guardar estado por número
                number_key = f"campaign:{campaign_name}:number:{numero}"
                pipe.set(number_key, json.dumps(call_data), ex=3600)  # TTL 1 hora
                
                # Incrementar contador de estado
                state_counter_key = f"campaign:{campaign_name}:state:{estado}"
                pipe.incr(state_counter_key)
                
                # Si es estado terminal, limpiar de activas
                if estado.upper() in TERMINAL_STATES:
                    if uuid:
                        pipe.hdel("global:active_calls", uuid)
                        # Expirar el registro después de 5 minutos
                        pipe.expire(f"campaign:{campaign_name}:calls", 300)
                
                # Ejecutar pipeline
                pipe.execute()
                logger.debug(f"✅ Redis: {campaign_name} - {numero} -> {estado}")
                
            except redis.RedisError as e:
                logger.warning(f"⚠️ Redis error (MySQL OK): {e}")
            except Exception as e:
                logger.warning(f"⚠️ Redis update error (MySQL OK): {e}")
        
        # ===== USAR RedisCallManager si está disponible =====
        if redis_manager:
            try:
                redis_manager.set_call_state(
                    campaign_name=campaign_name,
                    numero=numero,
                    uuid=uuid or "",
                    estado=estado,
                    metadata={
                        "duracion": duracion,
                        "hangup_reason": hangup_reason,
                        "amd_result": amd_result
                    }
                )
            except Exception as e:
                logger.debug(f"⚠️ RedisCallManager error: {e}")
                
    except Exception as e:
        logger.error(f"❌ Error actualizando estado: {campaign_name}/{numero}: {e}")


async def state_update_worker(worker_id: int):
    """
    Worker independiente para procesar actualizaciones de estado desde la cola
    """
    logger.info(f"🔧 State update worker #{worker_id} iniciado")
    loop = asyncio.get_event_loop()
    
    while True:
        try:
            # Obtener evento de la cola
            try:
                event_data = await asyncio.wait_for(EVENT_QUEUE.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            
            if event_data is None:  # Señal de terminar
                logger.info(f"🛑 State update worker #{worker_id} terminando")
                break
            
            # Procesar actualización de estado en ThreadPoolExecutor
            await loop.run_in_executor(
                DB_EXECUTOR,
                update_call_status_sync,
                event_data.get('campaign_name'),
                event_data.get('numero'),
                event_data.get('estado'),
                event_data.get('duracion', '0'),
                event_data.get('uuid'),
                event_data.get('hangup_reason'),
                event_data.get('amd_result'),
                event_data.get('incrementar_intento', False)
            )
            
            # Marcar tarea como completada
            EVENT_QUEUE.task_done()
            
            logger.debug(f"✅ Worker #{worker_id}: {event_data.get('campaign_name')}/{event_data.get('numero')} -> {event_data.get('estado')}")
            
        except Exception as e:
            logger.error(f"❌ Error en worker #{worker_id}: {e}")
            await asyncio.sleep(0.1)


async def queue_state_update(campaign_name: str, numero: str, estado: str, 
                            uuid: str = None, duracion: str = "0",
                            hangup_reason: str = None, amd_result: str = None,
                            incrementar_intento: bool = False):
    """
    Encola una actualización de estado sin bloquear
    """
    global EVENT_QUEUE
    
    if EVENT_QUEUE is None:
        logger.warning(f"⚠️ Cola no inicializada, procesando directo")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            DB_EXECUTOR,
            update_call_status_sync,
            campaign_name, numero, estado, duracion, uuid,
            hangup_reason, amd_result, incrementar_intento
        )
        return
    
    try:
        event_data = {
            'campaign_name': campaign_name,
            'numero': numero,
            'estado': estado,
            'uuid': uuid,
            'duracion': duracion,
            'hangup_reason': hangup_reason,
            'amd_result': amd_result,
            'incrementar_intento': incrementar_intento
        }
        
        await EVENT_QUEUE.put(event_data)
        logger.debug(f"📥 Encolado: {campaign_name}/{numero} -> {estado}")
        
    except asyncio.QueueFull:
        logger.warning(f"⚠️ Cola llena, procesando directo")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            DB_EXECUTOR,
            update_call_status_sync,
            campaign_name, numero, estado, duracion, uuid,
            hangup_reason, amd_result, incrementar_intento
        )
    except Exception as e:
        logger.error(f"❌ Error encolando estado: {e}")


def release_uuid(uuid: str, reason: str = ""):
    """Libera un UUID del tracking global"""
    global GLOBAL_ACTIVE_UUIDS, GLOBAL_UUID_TIMESTAMPS, answered_times
    
    with state_lock:
        if uuid in GLOBAL_ACTIVE_UUIDS:
            GLOBAL_ACTIVE_UUIDS.discard(uuid)
            logger.info(f"🔓 UUID liberado: {uuid} ({reason})")
        
        if uuid in GLOBAL_UUID_TIMESTAMPS:
            del GLOBAL_UUID_TIMESTAMPS[uuid]
        
        # Limpiar answered_times
        keys_to_remove = [key for key in answered_times.keys() if key[1] == uuid]
        for key in keys_to_remove:
            del answered_times[key]


async def handle_esl_event(con, event_str, campaign_name, stats_callback=None):
    """
    Procesa un evento ESL y actualiza estados
    """
    global answered_times
    
    destino_transfer = "9999"
    
    try:
        # Extraer campos del evento
        numero = extract_field(event_str, "origination_caller_id_number")
        if not numero:
            numero = extract_field(event_str, "variable_callee_id_number")
        if not numero:
            numero = extract_field(event_str, "Caller-Destination-Number")
        if not numero:
            numero = extract_field(event_str, "Caller-Caller-ID-Number")
        
        uuid = extract_field(event_str, "Unique-ID")
        event_campaign = extract_field(event_str, "variable_campaign_name")
        
        # Filtrar eventos de otras campañas
        if event_campaign and event_campaign != campaign_name:
            return False
        
        # Ignorar eventos sin número o de transferencia
        if numero == destino_transfer or not numero:
            return False
        
        # Verificar que el número pertenece a esta campaña
        if numero and uuid:
            try:
                with engine.connect() as conn:
                    count = conn.execute(
                        text(f"SELECT COUNT(*) FROM `{campaign_name}` WHERE telefono = :numero OR uuid = :uuid"),
                        {"numero": numero, "uuid": uuid}
                    ).scalar()
                    if count == 0:
                        return False
            except Exception as e:
                logger.debug(f"Error verificando número: {e}")
                return False
        
        key = (numero, uuid)
        
        # CHANNEL_PROGRESS / CHANNEL_PROGRESS_MEDIA
        if "CHANNEL_PROGRESS_MEDIA" in event_str or "CHANNEL_PROGRESS" in event_str:
            logger.info(f"🔔 [{campaign_name}] Llamada en progreso: {numero} ({uuid})")
            await queue_state_update(campaign_name, numero, "P", uuid=uuid)
            
            if WEBSOCKET_AVAILABLE:
                stats = get_campaign_stats_fast(campaign_name)
                await safe_send_to_websocket(send_event_to_websocket, "call_progress", {
                    "campaign": campaign_name,
                    "campaign_name": campaign_name,
                    "numero": numero,
                    "uuid": uuid,
                    "status": "in_progress",
                    "stats": stats
                })
            return True
        
        # CHANNEL_ANSWER
        elif "CHANNEL_ANSWER" in event_str:
            logger.info(f"📞 [{campaign_name}] Llamada contestada: {numero} (UUID: {uuid})")
            await queue_state_update(campaign_name, numero, "S", uuid=uuid)
            
            # Guardar timestamp para calcular duración
            with state_lock:
                answered_times[(campaign_name, uuid)] = time.time()
            
            # Obtener tipo de AMD y actualizar amd_result
            try:
                with engine.connect() as conn:
                    res = conn.execute(
                        text("SELECT amd FROM campanas WHERE nombre = :nombre"),
                        {"nombre": campaign_name}
                    ).fetchone()
                    amd_active = res[0] if res and res[0] else 'PRO'
                
                if str(amd_active).upper() == "PRO":
                    with engine.begin() as conn:
                        conn.execute(
                            text(f"UPDATE `{campaign_name}` SET amd_result = 'HUMAN' WHERE uuid = :uuid"),
                            {"uuid": uuid}
                        )
                        logger.info(f"✅ [{campaign_name}] AMD result: HUMAN para {numero}")
            except Exception as e:
                logger.error(f"❌ Error actualizando amd_result: {e}")
            
            if WEBSOCKET_AVAILABLE:
                stats = get_campaign_stats_fast(campaign_name)
                await safe_send_to_websocket(send_event_to_websocket, "call_answered", {
                    "campaign": campaign_name,
                    "campaign_name": campaign_name,
                    "numero": numero,
                    "uuid": uuid,
                    "stats": stats
                })
            return True
        
        # CUSTOM AMD events
        elif "CUSTOM" in event_str and "amd" in event_str.lower():
            amd_result = extract_field(event_str, "AMD-Result") or extract_field(event_str, "variable_amd_result")
            
            if amd_result:
                logger.info(f"🤖 [{campaign_name}] AMD Result: {amd_result} para {numero}")
                
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            text(f"UPDATE `{campaign_name}` SET amd_result = :amd_result WHERE uuid = :uuid"),
                            {"amd_result": amd_result, "uuid": uuid}
                        )
                except Exception as e:
                    logger.error(f"❌ Error actualizando amd_result: {e}")
                
                if amd_result.upper() == "MACHINE":
                    logger.info(f"🤖 [{campaign_name}] MÁQUINA detectada: {numero}")
                    await queue_state_update(
                        campaign_name, numero, "M", uuid=uuid, duracion="0",
                        hangup_reason="AMD_MACHINE", amd_result=amd_result
                    )
                
                if WEBSOCKET_AVAILABLE:
                    stats = get_campaign_stats_fast(campaign_name)
                    await safe_send_to_websocket(send_event_to_websocket, "amd_result", {
                        "campaign": campaign_name,
                        "campaign_name": campaign_name,
                        "numero": numero,
                        "uuid": uuid,
                        "amd_result": amd_result,
                        "stats": stats
                    })
            return True
        
        # CHANNEL_HANGUP_COMPLETE
        elif "CHANNEL_HANGUP_COMPLETE" in event_str:
            # Obtener duraciones y causa
            duracion_billsec = extract_field(event_str, "variable_billsec") or "0"
            amd_result = extract_field(event_str, "variable_amd_result") or ""
            causa = extract_field(event_str, "Hangup-Cause")
            hangup_reason = causa or extract_field(event_str, "variable_hangup_cause")
            
            # Liberar UUID inmediatamente
            release_uuid(uuid, f"HANGUP: {causa}")
            
            # Calcular duración
            duracion_final = "0"
            answered_at = None
            
            with state_lock:
                if (campaign_name, uuid) in answered_times:
                    answered_at = answered_times[(campaign_name, uuid)]
                    del answered_times[(campaign_name, uuid)]
            
            if answered_at is not None:
                real_duration = time.time() - answered_at
                duracion_final = str(max(0, math.floor(real_duration)))
                logger.info(f"🕐 [{campaign_name}] Duración calculada: {real_duration:.2f}s → {duracion_final}s")
            elif int(duracion_billsec) > 0:
                duracion_final = str(max(0, math.floor(float(duracion_billsec))))
            
            # Máquinas siempre duración 0
            if amd_result.upper() == "MACHINE":
                duracion_final = "0"
            
            logger.debug(f"🔚 [{campaign_name}] Hangup: causa='{causa}', duracion='{duracion_final}s'")
            
            # Verificar si ya fue marcada como máquina
            already_machine = False
            try:
                with engine.connect() as conn:
                    result = conn.execute(
                        text(f"SELECT estado FROM `{campaign_name}` WHERE uuid = :uuid"),
                        {"uuid": uuid}
                    ).fetchone()
                    if result and result[0] == "M":
                        already_machine = True
            except Exception:
                pass
            
            # Determinar estado final según la causa
            if causa == "NORMAL_CLEARING":
                if not already_machine:
                    # Verificar estado actual
                    try:
                        with engine.connect() as conn:
                            result = conn.execute(
                                text(f"SELECT estado FROM `{campaign_name}` WHERE uuid = :uuid"),
                                {"uuid": uuid}
                            ).fetchone()
                            estado_actual = result[0] if result else None
                        
                        if estado_actual == "S":
                            await queue_state_update(
                                campaign_name, numero, "C",
                                duracion=duracion_final, uuid=uuid,
                                hangup_reason=hangup_reason
                            )
                        else:
                            await queue_state_update(
                                campaign_name, numero, "C", uuid=uuid,
                                duracion="0", hangup_reason=hangup_reason
                            )
                    except Exception as e:
                        logger.error(f"Error verificando estado: {e}")
                        await queue_state_update(
                            campaign_name, numero, "C", uuid=uuid,
                            duracion=duracion_final, hangup_reason=hangup_reason
                        )
            
            elif causa in ["USER_BUSY", "CALL_REJECTED"]:
                logger.info(f"📵 [{campaign_name}] Línea ocupada: {numero}")
                await queue_state_update(
                    campaign_name, numero, "O", uuid=uuid,
                    duracion="0", hangup_reason=hangup_reason
                )
            
            elif causa in ["NO_ANSWER", "ORIGINATOR_CANCEL"]:
                logger.info(f"📴 [{campaign_name}] Sin respuesta: {numero}")
                await queue_state_update(
                    campaign_name, numero, "N", uuid=uuid,
                    duracion="0", hangup_reason=hangup_reason
                )
            
            elif causa == "NO_USER_RESPONSE":
                logger.info(f"📴 [{campaign_name}] Sin respuesta del usuario: {numero}")
                await queue_state_update(
                    campaign_name, numero, "U", uuid=uuid,
                    duracion="0", hangup_reason=hangup_reason
                )
            
            elif causa == "NORMAL_TEMPORARY_FAILURE":
                logger.info(f"🚫 [{campaign_name}] Sin canales disponibles: {numero}")
                await queue_state_update(
                    campaign_name, numero, "E", uuid=uuid,
                    duracion="0", hangup_reason=hangup_reason
                )
            
            elif causa == "NO_ROUTE_DESTINATION":
                logger.info(f"🗺️ [{campaign_name}] Sin ruta de destino: {numero}")
                await queue_state_update(
                    campaign_name, numero, "R", uuid=uuid,
                    duracion="0", hangup_reason=hangup_reason
                )
            
            elif causa == "UNALLOCATED_NUMBER":
                logger.info(f"❌ [{campaign_name}] Número inexistente: {numero}")
                await queue_state_update(
                    campaign_name, numero, "I", uuid=uuid,
                    duracion="0", hangup_reason=hangup_reason
                )
            
            elif causa == "INCOMPATIBLE_DESTINATION":
                logger.info(f"🔧 [{campaign_name}] Codecs incompatibles: {numero}")
                await queue_state_update(
                    campaign_name, numero, "X", uuid=uuid,
                    duracion="0", hangup_reason=hangup_reason
                )
            
            elif causa == "RECOVERY_ON_TIMER_EXPIRE":
                logger.info(f"⏰ [{campaign_name}] Timeout SIP: {numero}")
                await queue_state_update(
                    campaign_name, numero, "T", uuid=uuid,
                    duracion="0", hangup_reason=hangup_reason
                )
            
            else:
                logger.info(f"❌ [{campaign_name}] Llamada fallida: {numero}, Causa: {causa}")
                await queue_state_update(
                    campaign_name, numero, "E", uuid=uuid,
                    duracion="0", hangup_reason=hangup_reason
                )
            
            if WEBSOCKET_AVAILABLE:
                # Enviar evento de hangup con estadísticas actualizadas
                stats = get_campaign_stats_fast(campaign_name)
                await safe_send_to_websocket(send_event_to_websocket, "call_hangup", {
                    "campaign": campaign_name,
                    "campaign_name": campaign_name,
                    "numero": numero,
                    "uuid": uuid,
                    "causa": causa,
                    "duracion": duracion_final,
                    "stats": stats
                })
                # También enviar stats actualizadas
                await safe_send_to_websocket(send_stats_to_websocket, stats)
            
            return True
        
        return False
        
    except Exception as e:
        logger.error(f"Error procesando evento ESL: {e}")
        # Liberar UUID si hubo error
        if 'uuid' in locals() and uuid:
            release_uuid(uuid, f"EXCEPTION: {e}")
        return False


async def esl_event_listener(campaign_filter: str = None):
    """
    Listener principal de eventos ESL
    Se conecta a FreeSWITCH y escucha eventos de todas las campañas
    """
    logger.info("🎧 Iniciando listener de eventos ESL...")
    
    while True:
        try:
            con = ESL.ESLconnection(FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD)
            
            if not con.connected():
                logger.error("❌ No se pudo conectar a FreeSWITCH ESL")
                await asyncio.sleep(5)
                continue
            
            logger.info("✅ Conectado a FreeSWITCH ESL")
            
            # Suscribirse a eventos
            con.events("plain", "CHANNEL_ANSWER CHANNEL_HANGUP_COMPLETE CHANNEL_PROGRESS CHANNEL_PROGRESS_MEDIA CUSTOM")
            
            while con.connected():
                event = con.recvEventTimed(100)  # 100ms timeout
                
                if not event:
                    await asyncio.sleep(0.01)
                    continue
                
                event_str = event.serialize()
                
                # Obtener nombre de campaña del evento
                campaign_name = extract_field(event_str, "variable_campaign_name")
                
                if not campaign_name:
                    # Intentar obtener de UUID
                    uuid = extract_field(event_str, "Unique-ID")
                    if uuid and "_" in uuid:
                        campaign_name = uuid.split("_")[0]
                
                if not campaign_name:
                    continue
                
                # Filtrar si se especificó una campaña
                if campaign_filter and campaign_name != campaign_filter:
                    continue
                
                # Procesar evento
                await handle_esl_event(con, event_str, campaign_name)
            
            logger.warning("⚠️ Conexión ESL perdida, reconectando...")
            await asyncio.sleep(2)
            
        except Exception as e:
            logger.error(f"❌ Error en ESL listener: {e}")
            await asyncio.sleep(5)


async def cleanup_stale_uuids():
    """Limpia UUIDs que han estado activos por más de 1 minuto"""
    current_time = time.time()
    stale_uuids = []
    
    with state_lock:
        for uuid, timestamp in list(GLOBAL_UUID_TIMESTAMPS.items()):
            if current_time - timestamp > 60:
                stale_uuids.append(uuid)
    
    for uuid in stale_uuids:
        release_uuid(uuid, "stale")
    
    return len(stale_uuids)


async def periodic_cleanup():
    """Tarea de limpieza periódica"""
    while True:
        try:
            await asyncio.sleep(30)
            cleaned = await cleanup_stale_uuids()
            if cleaned > 0:
                logger.info(f"🧹 Limpieza periódica: {cleaned} UUIDs obsoletos removidos")
        except Exception as e:
            logger.error(f"❌ Error en limpieza periódica: {e}")


async def main():
    """Función principal del proceso de actualización de estados"""
    global EVENT_QUEUE
    
    logger.info("=" * 60)
    logger.info("🚀 STATE UPDATER - Proceso independiente")
    logger.info("=" * 60)
    
    # Mostrar estado de conexiones
    logger.info("")
    logger.info("📡 Estado de conexiones:")
    logger.info(f"   MySQL: ✅ Configurado ({DB_URL.split('@')[1] if '@' in DB_URL else DB_URL})")
    if REDIS_AVAILABLE:
        logger.info(f"   Redis: ✅ Conectado ({REDIS_HOST}:{REDIS_PORT})")
        if redis_manager:
            logger.info(f"   RedisCallManager: ✅ Disponible")
        else:
            logger.info(f"   RedisCallManager: ⚠️ No disponible (usando redis_client directo)")
    else:
        logger.info(f"   Redis: ❌ No disponible")
    logger.info("")
    
    # 🌐 WebSocket se inicia automáticamente cuando se envía el primer mensaje
    if WEBSOCKET_AVAILABLE:
        logger.info("🌐 WebSocket disponible - se iniciará automáticamente con el primer evento")
        # Enviar un evento inicial para forzar el inicio del servidor WebSocket
        try:
            await safe_send_to_websocket(send_event_to_websocket, "system_startup", {
                "process": "state_updater",
                "status": "starting",
                "timestamp": datetime.now().isoformat()
            })
            logger.info("✅ Servidor WebSocket iniciado (WS:8765, WSS:8766)")
            
            # Iniciar el monitoreo de base de datos para push automático de stats
            if ws_server_instance:
                await ws_server_instance.start_db_monitoring()
                logger.info("✅ Monitoreo de DB iniciado - push automático de stats activado")
        except Exception as e:
            logger.warning(f"⚠️ Error iniciando WebSocket: {e}")
    
    # Inicializar cola de eventos
    EVENT_QUEUE = asyncio.Queue(maxsize=20000)
    logger.info("✅ Cola de eventos inicializada (max: 20000)")
    
    # Iniciar workers de actualización
    logger.info(f"🔧 Iniciando {STATE_UPDATE_WORKERS} workers de actualización...")
    workers = []
    for i in range(STATE_UPDATE_WORKERS):
        worker = asyncio.create_task(state_update_worker(i))
        workers.append(worker)
    logger.info(f"✅ {STATE_UPDATE_WORKERS} workers iniciados")
    
    # Iniciar listener ESL
    esl_task = asyncio.create_task(esl_event_listener())
    
    # Iniciar limpieza periódica
    cleanup_task = asyncio.create_task(periodic_cleanup())
    
    logger.info("")
    logger.info("🎯 Sistema listo - Escuchando eventos ESL...")
    logger.info("💡 Presiona Ctrl+C para detener")
    logger.info("")
    
    try:
        # Esperar indefinidamente
        await asyncio.gather(esl_task, cleanup_task, *workers)
    except KeyboardInterrupt:
        logger.info("\n⏹️ Deteniendo state updater...")
    finally:
        # Detener workers
        for _ in range(STATE_UPDATE_WORKERS):
            await EVENT_QUEUE.put(None)
        await asyncio.gather(*workers, return_exceptions=True)
        logger.info("✅ Workers detenidos")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️ State Updater detenido")
    except Exception as e:
        logger.error(f"❌ Error fatal: {e}")
        sys.exit(1)