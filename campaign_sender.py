"""
Campaign Sender - Orquestador principal para todas las campañas
Monitorea y ejecuta campañas de todos los tipos usando los plugins correspondientes.
"""
import asyncio
import sys
import signal
import traceback
from typing import Dict, Set, Optional
from datetime import datetime, time as dt_time
from sqlalchemy import text

# Importar configuración compartida
from shared_config import (
    get_logger, create_db_engine, CampaignType,
    REDIS_HOST, REDIS_PORT, REDIS_DB
)

# Importar senders
from senders import (
    SENDER_REGISTRY, get_sender, get_available_types,
    BaseSender, SenderStats
)

logger = get_logger("campaign_sender")

# Pool de conexiones BD
engine = create_db_engine(pool_size=30, max_overflow=15)

# Redis opcional
REDIS_AVAILABLE = False
redis_client = None
try:
    import redis
    redis_client = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
        decode_responses=True, socket_connect_timeout=5
    )
    redis_client.ping()
    REDIS_AVAILABLE = True
    logger.info("✅ Redis conectado")
except:
    logger.warning("⚠️ Redis no disponible")

# WebSocket opcional
try:
    from websocket_server import send_stats_to_websocket, send_event_to_websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False
    async def send_stats_to_websocket(*args, **kwargs): pass
    async def send_event_to_websocket(*args, **kwargs): pass

# Control global
RUNNING = True
ACTIVE_CAMPAIGNS: Dict[str, BaseSender] = {}
CAMPAIGN_TASKS: Dict[str, asyncio.Task] = {}

# Configuración de workers por tipo
WORKERS_PER_TYPE = {
    'Audio': 3,
    'Discador': 2,
    'WhatsApp': 2,
    'Telegram': 2,
    'Facebook': 2,
    'Email': 3,
    'SMS': 2,
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
        logger.debug(f"Error verificando horario: {e}")
        return True


async def get_active_campaigns() -> list:
    """Obtiene campañas activas de todos los tipos"""
    campaigns = []
    
    try:
        with engine.connect() as conn:
            # Campañas activas
            result = conn.execute(text("""
                SELECT nombre, tipo, cps, reintentos, horarios
                FROM campanas
                WHERE activo = 'S'
                AND (fecha_programada IS NULL OR fecha_programada <= NOW())
            """)).fetchall()
            
            for row in result:
                nombre, tipo, cps, reintentos, horarios = row
                
                # Verificar que el tipo es soportado
                if tipo not in SENDER_REGISTRY:
                    logger.warning(f"⚠️ Tipo no soportado: {tipo} para campaña {nombre}")
                    continue
                
                # Verificar horario
                if not is_in_schedule(horarios):
                    continue
                
                campaigns.append({
                    "nombre": nombre,
                    "tipo": tipo,
                    "cps": cps or 10,
                    "reintentos": reintentos or 3,
                    "horarios": horarios
                })
            
            # Reactivar pausadas que entran en horario
            paused = conn.execute(text("""
                SELECT nombre, tipo, horarios
                FROM campanas
                WHERE activo = 'P'
            """)).fetchall()
            
            for row in paused:
                nombre, tipo, horarios = row
                if is_in_schedule(horarios):
                    logger.info(f"🔄 Reactivando campaña pausada: {nombre}")
                    with engine.begin() as c:
                        c.execute(
                            text("UPDATE campanas SET activo = 'S' WHERE nombre = :n"),
                            {"n": nombre}
                        )
    
    except Exception as e:
        logger.error(f"Error obteniendo campañas: {e}")
    
    return campaigns


async def process_campaign(campaign_info: dict):
    """Procesa una campaña individual"""
    nombre = campaign_info["nombre"]
    tipo = campaign_info["tipo"]
    
    logger.info(f"🚀 [{nombre}] Iniciando campaña tipo {tipo}")
    
    try:
        # Obtener clase de sender
        sender_class = get_sender(tipo)
        if not sender_class:
            logger.error(f"❌ No hay sender para tipo: {tipo}")
            return
        
        # Crear instancia del sender
        sender = sender_class(nombre, campaign_info)
        ACTIVE_CAMPAIGNS[nombre] = sender
        
        # Ejecutar
        stats = await sender.run()
        
        # Enviar estadísticas finales
        if WEBSOCKET_AVAILABLE:
            await send_stats_to_websocket(stats.to_dict())
        
        logger.info(f"✅ [{nombre}] Campaña finalizada")
        
    except asyncio.CancelledError:
        logger.info(f"⏹️ [{nombre}] Campaña cancelada")
    except Exception as e:
        logger.error(f"❌ [{nombre}] Error: {e}")
        logger.error(traceback.format_exc())
    finally:
        ACTIVE_CAMPAIGNS.pop(nombre, None)


async def check_campaign_completion(campaign_name: str, campaign_type: str) -> bool:
    """Verifica si una campaña debe finalizarse"""
    try:
        with engine.connect() as conn:
            # Contar pendientes
            result = conn.execute(text(f"""
                SELECT
                    COUNT(*) as total,
                    COUNT(CASE WHEN estado = 'pendiente' THEN 1 END) as pendientes,
                    COUNT(CASE WHEN estado IN ('P', 'R', 'A', 'Q', 'T', 'enviando') THEN 1 END) as activos
                FROM `{campaign_name}`
            """)).fetchone()
            
            if result:
                total, pendientes, activos = result
                
                # Si no hay pendientes ni activos, finalizar
                if pendientes == 0 and activos == 0 and total > 0:
                    logger.info(f"🏁 [{campaign_name}] Sin pendientes - Finalizando")
                    with engine.begin() as c:
                        c.execute(
                            text("UPDATE campanas SET activo = 'F', fecha_fin = NOW() WHERE nombre = :n"),
                            {"n": campaign_name}
                        )
                    return True
    except Exception as e:
        logger.debug(f"Error verificando completitud: {e}")
    
    return False


async def campaign_monitor():
    """Monitor principal que gestiona todas las campañas"""
    global RUNNING
    
    logger.info("🔍 Iniciando monitor de campañas...")
    logger.info(f"📋 Tipos soportados: {', '.join(get_available_types())}")
    
    while RUNNING:
        try:
            # Obtener campañas activas
            campaigns = await get_active_campaigns()
            
            for campaign in campaigns:
                nombre = campaign["nombre"]
                tipo = campaign["tipo"]
                
                # Verificar si ya está corriendo
                if nombre in ACTIVE_CAMPAIGNS:
                    continue
                
                # Verificar si existe la tabla
                try:
                    with engine.connect() as conn:
                        conn.execute(text(f"SELECT 1 FROM `{nombre}` LIMIT 1"))
                except:
                    logger.warning(f"⚠️ Tabla no existe: {nombre}")
                    continue
                
                # Verificar si tiene pendientes
                has_pending = False
                try:
                    with engine.connect() as conn:
                        result = conn.execute(text(f"""
                            SELECT COUNT(*) FROM `{nombre}`
                            WHERE estado = 'pendiente'
                            OR (estado IN ('N', 'O', 'F', 'E', 'failed') 
                                AND (intentos IS NULL OR intentos < :max))
                            LIMIT 1
                        """), {"max": campaign["reintentos"]}).scalar()
                        has_pending = result > 0
                except:
                    continue
                
                if has_pending:
                    # Crear task para la campaña
                    task = asyncio.create_task(process_campaign(campaign))
                    CAMPAIGN_TASKS[nombre] = task
                    logger.info(f"📦 [{nombre}] Encolada ({tipo})")
                else:
                    # Verificar si debe finalizarse
                    await check_campaign_completion(nombre, tipo)
            
            # Limpiar tasks finalizadas
            finished = [n for n, t in CAMPAIGN_TASKS.items() if t.done()]
            for n in finished:
                CAMPAIGN_TASKS.pop(n, None)
            
            # Esperar antes de siguiente ciclo
            await asyncio.sleep(5)
            
        except Exception as e:
            logger.error(f"❌ Error en monitor: {e}")
            await asyncio.sleep(5)


async def stats_broadcaster():
    """Envía estadísticas periódicas por WebSocket"""
    if not WEBSOCKET_AVAILABLE:
        return
    
    while RUNNING:
        try:
            for nombre, sender in list(ACTIVE_CAMPAIGNS.items()):
                stats = sender.stats.to_dict()
                await send_stats_to_websocket(stats)
            
            await asyncio.sleep(2)
        except:
            await asyncio.sleep(5)


async def shutdown():
    """Shutdown graceful"""
    global RUNNING
    RUNNING = False
    
    logger.info("⏹️ Deteniendo campañas...")
    
    # Detener todos los senders
    for nombre, sender in list(ACTIVE_CAMPAIGNS.items()):
        sender.stop()
    
    # Cancelar tasks
    for nombre, task in list(CAMPAIGN_TASKS.items()):
        task.cancel()
    
    # Esperar a que terminen
    if CAMPAIGN_TASKS:
        await asyncio.gather(*CAMPAIGN_TASKS.values(), return_exceptions=True)
    
    logger.info("✅ Shutdown completado")


async def main():
    """Función principal"""
    global RUNNING
    
    logger.info("=" * 70)
    logger.info("📢 CAMPAIGN SENDER - Orquestador Multi-Tipo")
    logger.info("=" * 70)
    logger.info("")
    logger.info("📋 Tipos de campaña soportados:")
    for tipo in get_available_types():
        workers = WORKERS_PER_TYPE.get(tipo, 2)
        logger.info(f"   • {tipo} ({workers} workers)")
    logger.info("")
    
    if REDIS_AVAILABLE:
        logger.info("✅ Redis: Conectado")
    else:
        logger.info("⚠️ Redis: No disponible")
    
    if WEBSOCKET_AVAILABLE:
        logger.info("✅ WebSocket: Disponible")
    else:
        logger.info("⚠️ WebSocket: No disponible")
    
    logger.info("")
    logger.info("🎯 Sistema listo - Monitoreando campañas...")
    logger.info("💡 Presiona Ctrl+C para detener")
    logger.info("")
    
    # Manejar señales
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        asyncio.create_task(shutdown())
    
    try:
        loop.add_signal_handler(signal.SIGINT, signal_handler)
        loop.add_signal_handler(signal.SIGTERM, signal_handler)
    except NotImplementedError:
        # Windows no soporta add_signal_handler
        pass
    
    try:
        # Iniciar monitor y broadcaster
        await asyncio.gather(
            campaign_monitor(),
            stats_broadcaster()
        )
    except KeyboardInterrupt:
        await shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️ Campaign Sender detenido")
    except Exception as e:
        logger.error(f"❌ Error fatal: {e}")
        sys.exit(1)
