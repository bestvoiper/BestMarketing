import asyncio
import websockets
import json
import logging
from datetime import datetime
import threading
from decimal import Decimal

# Custom JSON encoder para manejar Decimal
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# Sobrescribir json.dumps para usar el encoder personalizado
_original_dumps = json.dumps
def json_dumps(*args, **kwargs):
    kwargs.setdefault('cls', DecimalEncoder)
    return _original_dumps(*args, **kwargs)
json.dumps = json_dumps

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Importar Redis Manager
try:
    from redis_manager import get_redis_manager
    redis_manager = get_redis_manager()
    REDIS_AVAILABLE = True
    logger.info("✅ Redis Manager disponible para WebSocket")
except ImportError:
    REDIS_AVAILABLE = False
    redis_manager = None
    logger.warning("⚠️ Redis Manager no disponible - usando solo MySQL")

class StatsWebSocketServer:
    def __init__(self, host="0.0.0.0", port=8765):
        self.host = host
        self.port = port
        self.clients = set()
        self.current_stats = {
            "calls_sent": 0,
            "calls_ringing": 0,
            "calls_answered": 0,
            "calls_failed": 0,
            "calls_busy": 0,
            "calls_no_answer": 0,
            "timestamp": datetime.now().isoformat(),
            "campaign_name": "",
            "total_numbers": 0,
            "cps": 0
        }
        self.finished_campaigns = {}  # {campaign_name: [ {stats}, ... ]}
        self.finished_campaigns_lock = threading.Lock()
        self.finished_campaigns_expiry = {}  # {campaign_name: expiry_timestamp}
        self._server = None
        # Nuevos atributos para monitoreo en tiempo real instantáneo
        self.monitored_campaigns = {}  # {client_id: campaign_name}
        self.campaign_snapshots = {}  # {campaign_name: last_known_data}
        self.client_ids = {}  # {websocket: unique_client_id}
        self.next_client_id = 1
        self.campaign_change_triggers = {}  # {campaign_name: asyncio.Event}
        self.active_monitors = set()  # campanas actualmente monitoreadas
        # Nuevos atributos para detección de cambios en DB
        self.last_db_hash = None
        self.last_system_hash = None
        self.campaign_hashes = {}  # {campaign_name: hash}
        self.db_monitor_task = None
        self.recent_calls_monitors = {}  # {campaign_name: set(client_id)}

    async def get_database_status(self):
        """Obtiene el estado de las campanas desde la base de datos"""
        try:
            from sqlalchemy import create_engine, text as sql_text
            from decimal import Decimal
            # Pool optimizado para alta concurrencia
            engine = create_engine(
                "mysql+pymysql://consultas:consultas@localhost/masivos",
                pool_size=20,
                max_overflow=30,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            with engine.begin() as conn:
                # Obtener estado de campanas (excluyendo eliminadas) - LIMIT para performance
                campaigns_result = conn.execute(sql_text("""
                    SELECT nombre, activo, fechacarga
                    FROM campanas 
                    WHERE activo != 'E'
                    ORDER BY fechacarga DESC
                    LIMIT 100
                """))
                campaigns = []
                for row in campaigns_result:
                    campaign = dict(row._mapping)
                    # Convertir datetime objects y Decimals a tipos serializables
                    for key, value in campaign.items():
                        if hasattr(value, 'isoformat'):
                            campaign[key] = value.isoformat()
                        elif isinstance(value, Decimal):
                            campaign[key] = float(value)
                        elif value is None:
                            campaign[key] = None
                        else:
                            campaign[key] = str(value) if not isinstance(value, (int, float, bool, str)) else value
                    campaigns.append(campaign)
                
                # Para cada campaña, obtener estadísticas de su tabla específica
                for campaign in campaigns:
                    campaign_name = campaign['nombre']
                    try:
                        # Verificar si la tabla de la campaña existe
                        table_exists = conn.execute(sql_text("""
                            SELECT COUNT(*) 
                            FROM information_schema.tables 
                            WHERE table_schema = 'masivos' 
                            AND table_name = :table_name
                        """), {"table_name": campaign_name}).scalar()
                        
                        if table_exists:
                            # Obtener estadísticas de la tabla de la campaña
                            # Enviadas: cualquier estado que no sea 'pendiente' (se ha procesado)
                            stats_result = conn.execute(sql_text(f"""
                                SELECT 
                                    COUNT(CASE WHEN estado != 'pendiente' THEN 1 END) as calls_sent,
                                    COUNT(CASE WHEN estado = 'P' THEN 1 END) as calls_ringing,
                                    COUNT(CASE WHEN estado = 'S' THEN 1 END) as calls_answered,
                                    COUNT(CASE WHEN estado = 'C' THEN 1 END) as calls_completed,
                                    COUNT(CASE WHEN estado = 'E' THEN 1 END) as calls_failed,
                                    COUNT(CASE WHEN estado = 'O' THEN 1 END) as calls_busy,
                                    COUNT(CASE WHEN estado = 'N' OR estado = 'U' THEN 1 END) as calls_no_answer,
                                    COUNT(*) as total_calls,
                                    COUNT(CASE WHEN estado = 'pendiente' THEN 1 END) as calls_pending,
                                    COUNT(CASE WHEN estado = 'R' THEN 1 END) as calls_no_route,
                                    COUNT(CASE WHEN estado = 'I' THEN 1 END) as calls_invalid_number,
                                    COUNT(CASE WHEN estado = 'X' THEN 1 END) as calls_codec_error,
                                    COUNT(CASE WHEN estado = 'T' THEN 1 END) as calls_timeout,
                                    COUNT(CASE WHEN hangup_reason = 'server_hangup' THEN 1 END) as hangup_server,
                                    COUNT(CASE WHEN hangup_reason = 'client_hangup' THEN 1 END) as hangup_client,
                                    COUNT(CASE WHEN hangup_reason = 'client_cancel' OR hangup_reason = 'server_cancel' THEN 1 END) as calls_cancelled,
                                    MIN(fecha_envio) as campaign_start,
                                    MAX(fecha_envio) as campaign_end
                                FROM `{campaign_name}`
                            """))
                            stats = stats_result.fetchone()
                            if stats:
                                # Calcular duración de la campaña (tiempo transcurrido entre primera y última llamada)
                                campaign_start = stats[16]
                                campaign_end = stats[17]
                                
                                if campaign_start and campaign_end:
                                    # Calcular diferencia en segundos
                                    duration_delta = campaign_end - campaign_start
                                    total_duration_seconds = int(duration_delta.total_seconds())
                                else:
                                    total_duration_seconds = 0
                                
                                duration_minutes = total_duration_seconds // 60
                                duration_seconds = total_duration_seconds % 60
                                duration_hours = duration_minutes // 60
                                duration_minutes = duration_minutes % 60
                                
                                if duration_hours > 0:
                                    duration_formatted = f"{duration_hours}h {duration_minutes}m {duration_seconds}s"
                                else:
                                    duration_formatted = f"{duration_minutes}m {duration_seconds}s"
                                
                                campaign.update({
                                    'calls_sent': stats[0] or 0,
                                    'calls_ringing': stats[1] or 0,
                                    'calls_answered': stats[2] or 0,
                                    'calls_completed': stats[3] or 0,
                                    'calls_failed': stats[4] or 0,
                                    'calls_busy': stats[5] or 0,
                                    'calls_no_answer': stats[6] or 0,
                                    'total_calls': stats[7] or 0,
                                    'calls_pending': stats[8] or 0,
                                    'calls_no_route': stats[9] or 0,
                                    'calls_invalid_number': stats[10] or 0,
                                    'calls_codec_error': stats[11] or 0,
                                    'calls_timeout': stats[12] or 0,
                                    'hangup_server': stats[13] or 0,
                                    'hangup_client': stats[14] or 0,
                                    'calls_cancelled': stats[15] or 0,
                                    'total_duration': total_duration_seconds,
                                    'duration_formatted': duration_formatted
                                })
                            else:
                                campaign.update({
                                    'calls_sent': 0, 'calls_ringing': 0, 'calls_answered': 0,
                                    'calls_failed': 0, 'calls_busy': 0, 'calls_no_answer': 0,
                                    'total_calls': 0, 'calls_pending': 0, 'total_duration': 0,
                                    'duration_formatted': '0m 0s'
                                })
                        else:
                            campaign.update({
                                'calls_sent': 0, 'calls_ringing': 0, 'calls_answered': 0,
                                'calls_failed': 0, 'calls_busy': 0, 'calls_no_answer': 0,
                                'total_calls': 0, 'calls_pending': 0, 'total_duration': 0,
                                'duration_formatted': '0m 0s'
                            })
                    except Exception as e:
                        logger.error(f"Error obteniendo stats para campaña {campaign_name}: {e}")
                        campaign.update({
                            'calls_sent': 0, 'calls_ringing': 0, 'calls_answered': 0,
                            'calls_failed': 0, 'calls_busy': 0, 'calls_no_answer': 0,
                            'total_calls': 0, 'calls_pending': 0, 'total_duration': 0,
                            'duration_formatted': '0m 0s'
                        })
                
                return {
                    "status": "success",
                    "campaigns": campaigns,
                    "timestamp": datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error obteniendo estado de base de datos: {e}")
            return {
                "status": "error",
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            }

    async def get_system_status(self):
        """Obtiene el estado general del sistema"""
        try:
            from sqlalchemy import create_engine, text as sql_text
            engine = create_engine(
                "mysql+pymysql://consultas:consultas@localhost/masivos",
                pool_size=20,
                max_overflow=30,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            with engine.begin() as conn:
                # Contar campanas por estado
                active_campaigns = conn.execute(sql_text("""
                    SELECT COUNT(*) FROM campanas WHERE activo = 'S'
                """)).scalar() or 0
                
                inactive_campaigns = conn.execute(sql_text("""
                    SELECT COUNT(*) FROM campanas WHERE activo = 'N'
                """)).scalar() or 0
                
                total_campaigns = conn.execute(sql_text("""
                    SELECT COUNT(*) FROM campanas
                """)).scalar() or 0
                
                return {
                    "status": "success",
                    "system_status": "online",
                    "active_campaigns": active_campaigns,
                    "inactive_campaigns": inactive_campaigns,
                    "total_campaigns": total_campaigns,
                    "connected_clients": len(self.clients),
                    "server_uptime": datetime.now().isoformat(),
                    "timestamp": datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error obteniendo estado del sistema: {e}")
            return {
                "status": "error",
                "message": str(e),
                "system_status": "error",
                "connected_clients": len(self.clients),
                "timestamp": datetime.now().isoformat()
            }

    async def get_campaign_details(self, campaign_name):
        """Obtiene detalles específicos de una campaña - primero intenta desde Redis"""
        try:
            # 🔥 PRIMERO: Intentar obtener desde Redis si está disponible
            if REDIS_AVAILABLE and redis_manager:
                logger.debug(f"📡 Intentando obtener detalles de {campaign_name} desde Redis...")
                
                # Verificar si la campaña está en caché (activa o finalizada)
                is_in_cache = await redis_manager.is_campaign_in_cache(campaign_name)
                
                if not is_in_cache:
                    # 🔄 Campaña no en caché, verificar si está finalizada en MySQL y cargarla
                    logger.info(f"📥 Campaña {campaign_name} no en caché, intentando cargar desde MySQL...")
                    loaded = await redis_manager.load_finished_campaign_on_demand(campaign_name)
                    
                    if not loaded:
                        # Si no se pudo cargar, ir directo a MySQL (fallback completo)
                        logger.warning(f"⚠️ No se pudo cargar {campaign_name} en Redis, usando MySQL directo")
                        is_in_cache = False
                    else:
                        is_in_cache = True
                        logger.info(f"✅ Campaña {campaign_name} cargada en caché desde MySQL (24h TTL)")
                
                if is_in_cache:
                    # Obtener estadísticas desde Redis
                    stats = await redis_manager.get_campaign_stats(campaign_name)
                    
                    if stats:
                        logger.info(f"✅ Estadísticas de {campaign_name} obtenidas desde Redis (caché)")
                        
                        # Obtener datos de la campaña
                        campaign_key = redis_manager._get_campaign_key(campaign_name)
                        campaign_data_raw = redis_manager.redis_client.hgetall(campaign_key)
                        
                        campaign_dict = {}
                        if campaign_data_raw:
                            campaign_dict = {k: json.loads(v) for k, v in campaign_data_raw.items()}
                        
                    # Obtener llamadas recientes desde Redis (todas)
                    pattern = f"{redis_manager.PREFIX_CALL}{campaign_name}:*"
                    call_keys = redis_manager.redis_client.keys(pattern)
                    
                    recent_calls = []
                    for key in call_keys:
                        call_data = redis_manager.redis_client.hgetall(key)
                        if call_data:
                            try:
                                call_dict = {}
                                for k, v in call_data.items():
                                    # Decodificar si es bytes
                                    k_str = k.decode() if isinstance(k, bytes) else k
                                    v_str = v.decode() if isinstance(v, bytes) else v
                                    
                                    # Parsear JSON y manejar null correctamente
                                    try:
                                        parsed_value = json.loads(v_str)
                                        # Convertir null JSON a None Python
                                        call_dict[k_str] = parsed_value if parsed_value != 'null' else None
                                    except:
                                        call_dict[k_str] = v_str
                                
                                # Asegurar que tenga al menos el número de teléfono
                                if call_dict.get('telefono') or call_dict.get('numero'):
                                    recent_calls.append(call_dict)
                            except Exception as e:
                                logger.debug(f"Error parseando llamada desde Redis: {e}")
                                continue
                    
                    # Ordenar por fecha de envío de forma segura
                    def safe_sort_key(call):
                        try:
                            fecha = call.get('fecha_envio', '')
                            # Si es None o vacío, usar una fecha muy antigua
                            if not fecha or fecha == 'null':
                                return '1970-01-01 00:00:00'
                            # Si ya es string, devolverlo
                            if isinstance(fecha, str):
                                return fecha
                            # Si tiene método isoformat, usarlo
                            if hasattr(fecha, 'isoformat'):
                                return fecha.isoformat()
                            return str(fecha)
                        except Exception:
                            return '1970-01-01 00:00:00'
                    
                    recent_calls.sort(key=safe_sort_key, reverse=True)
                    
                    return {
                        "status": "success",
                        "campaign": campaign_dict,
                        "campaign_stats": stats,
                        "recent_calls": recent_calls,  # Todos los registros
                        "table_exists": True,
                        "source": "redis_cache",
                        "timestamp": datetime.now().isoformat()
                    }            # 🔄 FALLBACK: Si no está en Redis, obtener desde MySQL
            logger.debug(f"🔄 Obteniendo detalles de {campaign_name} desde MySQL...")
            from sqlalchemy import create_engine, text as sql_text
            engine = create_engine(
                "mysql+pymysql://consultas:consultas@localhost/masivos",
                pool_size=20,
                max_overflow=30,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            with engine.begin() as conn:
                # Detalles de la campaña
                campaign_result = conn.execute(sql_text("""
                    SELECT * FROM campanas WHERE nombre = :campaign
                """), {"campaign": campaign_name})
                campaign = campaign_result.fetchone()
                
                if not campaign:
                    return {"status": "error", "message": "Campaña no encontrada"}
                
                # Verificar si la tabla de la campaña existe
                table_exists = conn.execute(sql_text("""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = 'masivos' 
                    AND table_name = :table_name
                """), {"table_name": campaign_name}).scalar()
                
                calls = []
                campaign_stats = {
                    "total_calls": 0,
                    "calls_sent": 0,
                    "calls_ringing": 0,
                    "calls_answered": 0,
                    "calls_failed": 0,
                    "calls_busy": 0,
                    "calls_no_answer": 0,
                    "calls_pending": 0
                }
                
                if table_exists:
                    # Obtener estadísticas de la campaña incluyendo nuevos estados específicos
                    stats_result = conn.execute(sql_text(f"""
                        SELECT 
                            COUNT(*) as total_calls,
                            COUNT(CASE WHEN estado != 'pendiente' THEN 1 END) as calls_sent,
                            COUNT(CASE WHEN estado = 'P' THEN 1 END) as calls_ringing,
                            COUNT(CASE WHEN estado = 'S' THEN 1 END) as calls_answered,
                            COUNT(CASE WHEN estado = 'C' THEN 1 END) as calls_completed,
                            COUNT(CASE WHEN estado = 'E' THEN 1 END) as calls_failed,
                            COUNT(CASE WHEN estado = 'O' THEN 1 END) as calls_busy,
                            COUNT(CASE WHEN estado = 'N' OR estado = 'U' THEN 1 END) as calls_no_answer,
                            COUNT(CASE WHEN estado = 'pendiente' THEN 1 END) as calls_pending,
                            COUNT(CASE WHEN amd_result = 'HUMAN' THEN 1 END) as human_calls,
                            COUNT(CASE WHEN amd_result = 'MACHINE' THEN 1 END) as machine_calls,
                            COUNT(CASE WHEN amd_result = 'NOTSURE' THEN 1 END) as notsure_calls,
                            COUNT(CASE WHEN estado = 'R' THEN 1 END) as calls_no_route,
                            COUNT(CASE WHEN estado = 'I' THEN 1 END) as calls_invalid_number,
                            COUNT(CASE WHEN estado = 'X' THEN 1 END) as calls_codec_error,
                            COUNT(CASE WHEN estado = 'T' THEN 1 END) as calls_timeout,
                            COUNT(CASE WHEN hangup_reason = 'server_hangup' THEN 1 END) as hangup_server,
                            COUNT(CASE WHEN hangup_reason = 'client_hangup' THEN 1 END) as hangup_client,
                            COUNT(CASE WHEN hangup_reason = 'client_cancel' OR hangup_reason = 'server_cancel' THEN 1 END) as calls_cancelled
                        FROM `{campaign_name}`
                    """))
                    stats = stats_result.fetchone()
                    if stats:
                        campaign_stats = {
                            "total_calls": stats[0] or 0,
                            "calls_sent": stats[1] or 0,
                            "calls_ringing": stats[2] or 0,
                            "calls_answered": stats[3] or 0,
                            "calls_completed": stats[4] or 0,
                            "calls_failed": stats[5] or 0,
                            "calls_busy": stats[6] or 0,
                            "calls_no_answer": stats[7] or 0,
                            "calls_pending": stats[8] or 0,
                            "amd_human": stats[9] or 0,
                            "amd_machine": stats[10] or 0,
                            "amd_notsure": stats[11] or 0,
                            "calls_no_route": stats[12] or 0,
                            "calls_invalid_number": stats[13] or 0,
                            "calls_codec_error": stats[14] or 0,
                            "calls_timeout": stats[15] or 0,
                            "hangup_server": stats[16] or 0,
                            "hangup_client": stats[17] or 0,
                            "calls_cancelled": stats[18] or 0
                        }
                    
                    # Llamadas de la tabla de la campaña incluyendo hangup_reason (LIMIT para performance)
                    calls_result = conn.execute(sql_text(f"""
                        SELECT telefono as numero, estado, fecha_envio as fecha_llamada, duracion, intentos, amd_result, hangup_reason
                        FROM `{campaign_name}` 
                        ORDER BY fecha_envio DESC
                        LIMIT 500
                    """))
                    calls = []
                    for row in calls_result:
                        call_dict = dict(row._mapping)
                        # Convertir datetime a string si existe
                        if 'fecha_llamada' in call_dict and call_dict['fecha_llamada']:
                            if hasattr(call_dict['fecha_llamada'], 'isoformat'):
                                call_dict['fecha_llamada'] = call_dict['fecha_llamada'].isoformat()
                            else:
                                call_dict['fecha_llamada'] = str(call_dict['fecha_llamada'])
                        calls.append(call_dict)
                
                # Convertir datetime objects en campaign
                campaign_dict = dict(campaign._mapping)
                for key, value in campaign_dict.items():
                    if hasattr(value, 'isoformat'):
                        campaign_dict[key] = value.isoformat()
                    elif value is None:
                        campaign_dict[key] = None
                    else:
                        campaign_dict[key] = str(value) if not isinstance(value, (int, float, bool, str)) else value
                
                return {
                    "status": "success",
                    "campaign": campaign_dict,
                    "campaign_stats": campaign_stats,
                    "recent_calls": calls,
                    "table_exists": bool(table_exists),
                    "timestamp": datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error obteniendo detalles de campaña {campaign_name}: {e}")
            return {
                "status": "error",
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            }

    async def handle_client_message(self, websocket, message):
        """Maneja mensajes recibidos del cliente"""
        try:
            data = json.loads(message)
            message_type = data.get("type")
            
            if message_type == "get_database_status":
                db_status = await self.get_database_status()
                response = {
                    "type": "database_status",
                    "data": db_status
                }
                await websocket.send(json.dumps(response))
                
            elif message_type == "get_campaign_details":
                campaign_name = data.get("campaign_name")
                if campaign_name:
                    details = await self.get_campaign_details(campaign_name)
                    response = {
                        "type": "campaign_details",
                        "data": details
                    }
                    await websocket.send(json.dumps(response))
                    
                    # Activar monitoreo automáticamente para esta campaña y cliente
                    if websocket in self.client_ids:
                        client_id = self.client_ids[websocket]
                        # Solo activar si no está ya monitoreando otra campaña
                        if client_id not in self.monitored_campaigns:
                            self.monitored_campaigns[client_id] = campaign_name
                            logger.info(f"🔄 Monitoreo automático activado para cliente {client_id} en campaña: {campaign_name}")
                    
            elif message_type == "monitor_campaign":
                campaign_name = data.get("campaign_name")
                if campaign_name and websocket in self.client_ids:
                    client_id = self.client_ids[websocket]
                    self.monitored_campaigns[client_id] = campaign_name
                    logger.info(f"Cliente {client_id} monitoreando campaña: {campaign_name}")
                    
                    # Enviar datos iniciales
                    details = await self.get_campaign_details(campaign_name)
                    response = {
                        "type": "campaign_details_live",
                        "data": details,
                        "monitored": True
                    }
                    await websocket.send(json.dumps(response))
                    
            elif message_type == "stop_monitor_campaign":
                if websocket in self.client_ids:
                    client_id = self.client_ids[websocket]
                    if client_id in self.monitored_campaigns:
                        campaign_name = self.monitored_campaigns[client_id]
                        del self.monitored_campaigns[client_id]
                        logger.info(f"Cliente {client_id} dejó de monitorear campaña: {campaign_name}")
                    
            elif message_type == "get_system_status":
                system_status = await self.get_system_status()
                response = {
                    "type": "system_status",
                    "data": system_status
                }
                await websocket.send(json.dumps(response))
                
            elif message_type == "ping":
                response = {
                    "type": "pong",
                    "timestamp": datetime.now().isoformat(),
                    "server_status": "online"
                }
                await websocket.send(json.dumps(response))
                
            elif message_type == "get_recent_calls":
                campaign_name = data.get("campaign_name")
                live = data.get("live", False)
                if campaign_name:
                    from sqlalchemy import create_engine, text as sql_text
                    engine = create_engine(
                        "mysql+pymysql://consultas:consultas@localhost/masivos",
                        pool_size=20,
                        max_overflow=30,
                        pool_pre_ping=True,
                        pool_recycle=3600
                    )
                    
                    with engine.begin() as conn:
                        # Obtener estadísticas actualizadas incluyendo AMD y estados específicos
                        stats_result = conn.execute(sql_text(f"""
                            SELECT 
                                COUNT(*) as total_calls,
                                COUNT(CASE WHEN estado != 'pendiente' THEN 1 END) as calls_sent,
                                COUNT(CASE WHEN estado = 'P' THEN 1 END) as calls_ringing,
                                COUNT(CASE WHEN estado = 'S' THEN 1 END) as calls_answered,
                                COUNT(CASE WHEN estado = 'C' THEN 1 END) as calls_completed,
                                COUNT(CASE WHEN estado = 'E' THEN 1 END) as calls_failed,
                                COUNT(CASE WHEN estado = 'O' THEN 1 END) as calls_busy,
                                COUNT(CASE WHEN estado = 'N' OR estado = 'U' THEN 1 END) as calls_no_answer,
                                COUNT(CASE WHEN estado = 'pendiente' THEN 1 END) as calls_pending,
                                COUNT(CASE WHEN amd_result = 'HUMAN' THEN 1 END) as human_calls,
                                COUNT(CASE WHEN amd_result = 'MACHINE' THEN 1 END) as machine_calls,
                                COUNT(CASE WHEN amd_result = 'NOTSURE' THEN 1 END) as notsure_calls,
                                COUNT(CASE WHEN estado = 'R' THEN 1 END) as calls_no_route,
                                COUNT(CASE WHEN estado = 'I' THEN 1 END) as calls_invalid_number,
                                COUNT(CASE WHEN estado = 'X' THEN 1 END) as calls_codec_error,
                                COUNT(CASE WHEN estado = 'T' THEN 1 END) as calls_timeout,
                                COUNT(CASE WHEN hangup_reason = 'server_hangup' THEN 1 END) as hangup_server,
                                COUNT(CASE WHEN hangup_reason = 'client_hangup' THEN 1 END) as hangup_client,
                                COUNT(CASE WHEN hangup_reason = 'client_cancel' OR hangup_reason = 'server_cancel' THEN 1 END) as calls_cancelled
                            FROM `{campaign_name}`
                        """))
                        stats = stats_result.fetchone()
                        campaign_stats = {
                            "total_calls": stats[0] or 0,
                            "calls_sent": stats[1] or 0,
                            "calls_ringing": stats[2] or 0,
                            "calls_answered": stats[3] or 0,
                            "calls_completed": stats[4] or 0,
                            "calls_failed": stats[5] or 0,
                            "calls_busy": stats[6] or 0,
                            "calls_no_answer": stats[7] or 0,
                            "calls_pending": stats[8] or 0,
                            "amd_human": stats[9] or 0,
                            "amd_machine": stats[10] or 0,
                            "amd_notsure": stats[11] or 0,
                            "calls_no_route": stats[12] or 0,
                            "calls_invalid_number": stats[13] or 0,
                            "calls_codec_error": stats[14] or 0,
                            "calls_timeout": stats[15] or 0,
                            "hangup_server": stats[16] or 0,
                            "hangup_client": stats[17] or 0,
                            "calls_cancelled": stats[18] or 0
                        }
                    
                    details = await self.get_campaign_details(campaign_name)
                    recent_calls = details.get("recent_calls", [])
                    response = {
                        "type": "recent_calls",
                        "campaign_name": campaign_name,
                        "recent_calls": recent_calls,
                        "campaign_stats": campaign_stats,
                        "timestamp": datetime.now().isoformat()
                    }
                    await websocket.send(json.dumps(response))
                    # Si el cliente pide actualizaciones en vivo, lo registramos
                    if live and websocket in self.client_ids:
                        client_id = self.client_ids[websocket]
                        self.recent_calls_monitors.setdefault(campaign_name, set()).add(client_id)
                    

        except json.JSONDecodeError:
            logger.error("Mensaje JSON inválido recibido del cliente")
        except Exception as e:
            logger.error(f"Error manejando mensaje del cliente: {e}")

    async def get_database_hash(self):
        """Obtiene un hash de los datos importantes de la base de datos para detectar cambios"""
        try:
            from sqlalchemy import create_engine, text as sql_text
            engine = create_engine(
                "mysql+pymysql://consultas:consultas@localhost/masivos",
                pool_size=20,
                max_overflow=30,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            with engine.begin() as conn:
                # Hash basado en datos importantes que queremos monitorear (excluyendo eliminadas)
                result = conn.execute(sql_text("""
                    SELECT 
                        COUNT(*) as total_campaigns,
                        COUNT(CASE WHEN activo = 'S' THEN 1 END) as active_campaigns,
                        COUNT(CASE WHEN activo = 'N' THEN 1 END) as inactive_campaigns,
                        MAX(fechacarga) as last_modified
                    FROM campanas
                    WHERE activo != 'E'
                """))
                result = result.fetchone()
                # Para cada campaña activa, obtener un hash de sus estadísticas
                campaigns_result = conn.execute(sql_text("""
                    SELECT nombre FROM campanas WHERE activo != 'E' ORDER BY nombre
                """))
                campaign_stats = []
                for row in campaigns_result:
                    campaign_name = row[0]
                    try:
                        # Verificar si la tabla existe
                        table_exists = conn.execute(sql_text("""
                            SELECT COUNT(*) 
                            FROM information_schema.tables 
                            WHERE table_schema = 'masivos' 
                            AND table_name = :table_name
                        """), {"table_name": campaign_name}).scalar()
                        
                        if table_exists:
                            # Obtener estadísticas principales
                            stats = conn.execute(sql_text(f"""
                                SELECT 
                                    COUNT(*) as total,
                                    COUNT(CASE WHEN estado != 'pendiente' THEN 1 END) as sent,
                                    COUNT(CASE WHEN estado = 'S' OR estado = 'C' THEN 1 END) as answered,
                                    MAX(fecha_envio) as last_call
                                FROM `{campaign_name}`
                            """)).fetchone()
                            campaign_stats.append(f"{campaign_name}:{stats[0]}:{stats[1]}:{stats[2]}")
                        else:
                            campaign_stats.append(f"{campaign_name}:0:0:0")
                    except:
                        campaign_stats.append(f"{campaign_name}:0:0:0")
                
                # Crear hash único basado en todos los datos
                hash_data = f"{result[0]}:{result[1]}:{result[2]}:{result[3]}:{'|'.join(campaign_stats)}"
                return hash(hash_data)
                
        except Exception as e:
            logger.error(f"Error obteniendo hash de base de datos: {e}")
            return None

    async def get_campaign_hash(self, campaign_name):
        """Obtiene un hash específico de una campaña para detectar cambios"""
        try:
            from sqlalchemy import create_engine, text as sql_text
            engine = create_engine(
                "mysql+pymysql://consultas:consultas@localhost/masivos",
                pool_size=20,
                max_overflow=30,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            with engine.begin() as conn:
                # Hash de la campaña en la tabla principal (excluyendo eliminadas)
                campaign_result = conn.execute(sql_text("""
                    SELECT activo, fechacarga, reintentos 
                    FROM campanas WHERE nombre = :campaign AND activo != 'E'
                """), {"campaign": campaign_name}).fetchone()
                
                if not campaign_result:
                    return None
                
                # Hash de la tabla de llamadas si existe
                table_exists = conn.execute(sql_text("""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = 'masivos' 
                    AND table_name = :table_name
                """), {"table_name": campaign_name}).scalar()
                
                calls_hash = "0:0:0:0"
                if table_exists:
                    calls_result = conn.execute(sql_text(f"""
                        SELECT 
                            COUNT(*) as total,
                            COUNT(CASE WHEN estado != 'pendiente' THEN 1 END) as sent,
                            COUNT(CASE WHEN estado = 'S' OR estado = 'C' THEN 1 END) as answered,
                            COUNT(CASE WHEN estado = 'E' THEN 1 END) as failed,
                            MAX(fecha_envio) as last_call
                        FROM `{campaign_name}`
                    """)).fetchone()
                    calls_hash = f"{calls_result[0]}:{calls_result[1]}:{calls_result[2]}:{calls_result[3]}"
                
                # Crear hash único
                hash_data = f"{campaign_result[0]}:{campaign_result[1]}:{campaign_result[2]}:{calls_hash}"
                return hash(hash_data)
                
        except Exception as e:
            logger.error(f"Error obteniendo hash de campaña {campaign_name}: {e}")
            return None

    async def start_db_monitoring(self):
        """Inicia el monitoreo continuo de cambios en la base de datos"""
        if self.db_monitor_task and not self.db_monitor_task.done():
            return
        
        self.db_monitor_task = asyncio.create_task(self._monitor_database_changes())
        logger.info("🔍 Iniciado monitoreo de cambios en base de datos")

    async def _monitor_database_changes(self):
        while True:
            try:
                if not self.clients:
                    await asyncio.sleep(5)  # Si no hay clientes, revisar menos frecuentemente
                    continue
                
                # Verificar cambios en database status general
                current_db_hash = await self.get_database_hash()
                if current_db_hash and current_db_hash != self.last_db_hash:
                    logger.info("🔄 Cambios detectados en base de datos - Enviando push")
                    self.last_db_hash = current_db_hash
                    
                    # Enviar database status actualizado
                    db_status = await self.get_database_status()
                    message = json.dumps({
                        "type": "database_status",
                        "data": db_status,
                        "change_detected": True
                    })
                    await self._broadcast(message)
                    
                    # También enviar system status actualizado
                    system_status = await self.get_system_status()
                    message = json.dumps({
                        "type": "system_status", 
                        "data": system_status,
                        "change_detected": True
                    })
                    await self._broadcast(message)
                
                # Verificar cambios en campanas específicas que están siendo monitoreadas
                monitored_campaigns = set(self.monitored_campaigns.values())
                # También incluir campanas con clientes suscritos a recent_calls en vivo
                live_recent_calls_campaigns = set(self.recent_calls_monitors.keys())
                all_monitored = monitored_campaigns | live_recent_calls_campaigns
                for campaign_name in all_monitored:
                    current_hash = await self.get_campaign_hash(campaign_name)
                    if current_hash and current_hash != self.campaign_hashes.get(campaign_name):
                        logger.info(f"🔄 Cambios detectados en campaña {campaign_name} - Enviando push")
                        self.campaign_hashes[campaign_name] = current_hash
                        details = await self.get_campaign_details(campaign_name)
                        # Notificar clientes de campaign_details_live
                        clients_to_notify = [
                            client_id for client_id, monitored_campaign 
                            in self.monitored_campaigns.items() 
                            if monitored_campaign == campaign_name
                        ]
                        if clients_to_notify:
                            await self._notify_campaign_change(campaign_name, details, clients_to_notify)
                        # Notificar clientes suscritos a recent_calls en vivo
                        recent_calls_clients = self.recent_calls_monitors.get(campaign_name, set())
                        if recent_calls_clients:
                            await self._notify_recent_calls_change(campaign_name, details.get("recent_calls", []), recent_calls_clients)
                
                # 🏁 Verificar finalización automática de campanas activas
                try:
                    from sqlalchemy import create_engine, text as sql_text
                    engine = create_engine(
                        "mysql+pymysql://consultas:consultas@localhost/masivos",
                        pool_size=20,
                        max_overflow=30,
                        pool_pre_ping=True,
                        pool_recycle=3600
                    )
                    with engine.begin() as conn:
                        # Obtener todas las campanas activas (S), excluyendo eliminadas
                        active_campaigns_result = conn.execute(sql_text("""
                            SELECT nombre FROM campanas WHERE activo = 'S' AND activo != 'E'
                        """))
                        
                        for row in active_campaigns_result:
                            campaign_name = row[0]
                            # Verificar si la campaña debe ser finalizada
                            details = await self.get_campaign_details(campaign_name)
                            if details.get("status") == "success" and details.get("table_exists"):
                                campaign_stats = details.get("campaign_stats", {})
                                total_calls = campaign_stats.get("total_calls", 0)
                                calls_pending = campaign_stats.get("calls_pending", 0)
                                calls_ringing = campaign_stats.get("calls_ringing", 0)
                                calls_answered = campaign_stats.get("calls_answered", 0)  # Estado S (activas)
                                
                                # Si hay llamadas en la campaña pero no hay pendientes, sonando o activas
                                if total_calls > 0 and calls_pending == 0 and calls_ringing == 0 and calls_answered == 0:
                                    logger.info(f"🏁 Campaña {campaign_name} completada - finalizando automáticamente")
                                    logger.info(f"   📊 Total: {total_calls}, Pendientes: {calls_pending}, Sonando: {calls_ringing}, Activas: {calls_answered}")
                                    
                                    # Marcar la campaña como finalizada en la BD (activo = 'F')
                                    try:
                                        conn.execute(sql_text(
                                            "UPDATE campanas SET activo = 'F' WHERE nombre = :campaign"
                                        ), {"campaign": campaign_name})
                                        
                                        await self.broadcast_event("campaign_finished", {
                                            "campaign_name": campaign_name,
                                            "message": "Campaña finalizada automáticamente - todas las llamadas completadas",
                                            "stats": campaign_stats,
                                            "timestamp": datetime.now().isoformat()
                                        })
                                        
                                        # Limpiar de monitoreos
                                        self.campaign_hashes.pop(campaign_name, None)
                                        self.recent_calls_monitors.pop(campaign_name, None)
                                        for client_id, monitored in list(self.monitored_campaigns.items()):
                                            if monitored == campaign_name:
                                                del self.monitored_campaigns[client_id]
                                                
                                    except Exception as e:
                                        logger.error(f"Error marcando campaña '{campaign_name}' como finalizada (F): {e}")
                                        
                except Exception as e:
                    logger.error(f"Error verificando finalización de campanas: {e}")
                
                # Esperar antes de la siguiente verificación
                await asyncio.sleep(5)  # Verificar cada 5 segundos (optimizado para 1000 concurrentes)
                
            except Exception as e:
                logger.error(f"Error en monitoreo de base de datos: {e}")
                await asyncio.sleep(5)

    async def _notify_campaign_change(self, campaign_name, campaign_data, client_ids_to_notify):
        """Notifica cambios de campaña solo a clientes específicos"""
        message = {
            "type": "campaign_details_live",
            "data": campaign_data,
            "campaign_name": campaign_name,
            "timestamp": datetime.now().isoformat(),
            "change_detected": True,
            "instant_update": True
        }
        
        message_json = json.dumps(message)
        disconnected_clients = []
        
        sent_count = 0
        for websocket in self.clients:
            if websocket in self.client_ids:
                client_id = self.client_ids[websocket]
                if client_id in client_ids_to_notify:
                    try:
                        await websocket.send(message_json)
                        sent_count += 1
                    except Exception as e:
                        logger.error(f"Error enviando actualización a cliente {client_id}: {e}")
                        disconnected_clients.append(websocket)
        
        if sent_count > 0:
            logger.info(f"⚡ Push enviado a {sent_count} clientes para {campaign_name}")
        
        # Limpiar clientes desconectados
        for websocket in disconnected_clients:
            self.clients.discard(websocket)
            if websocket in self.client_ids:
                client_id = self.client_ids[websocket]
                if client_id in self.monitored_campaigns:
                    del self.monitored_campaigns[client_id]
                del self.client_ids[websocket]

    async def _notify_recent_calls_change(self, campaign_name, recent_calls, client_ids_to_notify):
        """Notifica cambios de recent_calls solo a clientes suscritos"""
        # Obtener las estadísticas actualizadas de la campaña
        try:
            from sqlalchemy import create_engine, text as sql_text
            engine = create_engine("mysql+pymysql://consultas:consultas@localhost/masivos")
            
            with engine.begin() as conn:
                # Obtener estadísticas actualizadas incluyendo AMD y estados específicos
                stats_result = conn.execute(sql_text(f"""
                    SELECT 
                        COUNT(*) as total_calls,
                        COUNT(CASE WHEN estado != 'pendiente' THEN 1 END) as calls_sent,
                        COUNT(CASE WHEN estado = 'P' THEN 1 END) as calls_ringing,
                        COUNT(CASE WHEN estado = 'S' THEN 1 END) as calls_answered,
                        COUNT(CASE WHEN estado = 'C' THEN 1 END) as calls_completed,
                        COUNT(CASE WHEN estado = 'E' THEN 1 END) as calls_failed,
                        COUNT(CASE WHEN estado = 'O' THEN 1 END) as calls_busy,
                        COUNT(CASE WHEN estado = 'N' OR estado = 'U' THEN 1 END) as calls_no_answer,
                        COUNT(CASE WHEN estado = 'pendiente' THEN 1 END) as calls_pending,
                        COUNT(CASE WHEN amd_result = 'HUMAN' THEN 1 END) as human_calls,
                        COUNT(CASE WHEN amd_result = 'MACHINE' THEN 1 END) as machine_calls,
                        COUNT(CASE WHEN amd_result = 'NOTSURE' THEN 1 END) as notsure_calls,
                        COUNT(CASE WHEN estado = 'R' THEN 1 END) as calls_no_route,
                        COUNT(CASE WHEN estado = 'I' THEN 1 END) as calls_invalid_number,
                        COUNT(CASE WHEN estado = 'X' THEN 1 END) as calls_codec_error,
                        COUNT(CASE WHEN estado = 'T' THEN 1 END) as calls_timeout,
                        COUNT(CASE WHEN hangup_reason = 'server_hangup' THEN 1 END) as hangup_server,
                        COUNT(CASE WHEN hangup_reason = 'client_hangup' THEN 1 END) as hangup_client,
                        COUNT(CASE WHEN hangup_reason = 'client_cancel' OR hangup_reason = 'server_cancel' THEN 1 END) as calls_cancelled
                    FROM `{campaign_name}`
                """))
                stats = stats_result.fetchone()
                campaign_stats = {
                    "total_calls": stats[0] or 0,
                    "calls_sent": stats[1] or 0,
                    "calls_ringing": stats[2] or 0,
                    "calls_answered": stats[3] or 0,
                    "calls_completed": stats[4] or 0,
                    "calls_failed": stats[5] or 0,
                    "calls_busy": stats[6] or 0,
                    "calls_no_answer": stats[7] or 0,
                    "calls_pending": stats[8] or 0,
                    "amd_human": stats[9] or 0,
                    "amd_machine": stats[10] or 0,
                    "amd_notsure": stats[11] or 0,
                    "calls_no_route": stats[12] or 0,
                    "calls_invalid_number": stats[13] or 0,
                    "calls_codec_error": stats[14] or 0,
                    "calls_timeout": stats[15] or 0,
                    "hangup_server": stats[16] or 0,
                    "hangup_client": stats[17] or 0,
                    "calls_cancelled": stats[18] or 0
                }
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas para recent_calls de {campaign_name}: {e}")
            campaign_stats = {
                "total_calls": 0, "calls_sent": 0, "calls_ringing": 0,
                "calls_answered": 0, "calls_failed": 0, "calls_busy": 0,
                "calls_no_answer": 0, "calls_pending": 0,
                "amd_human": 0, "amd_machine": 0, "amd_notsure": 0
            }
        
        message = {
            "type": "recent_calls",
            "campaign_name": campaign_name,
            "recent_calls": recent_calls,
            "campaign_stats": campaign_stats,
            "timestamp": datetime.now().isoformat(),
            "change_detected": True,
            "instant_update": True
        }
        message_json = json.dumps(message)
        disconnected_clients = []
        sent_count = 0
        for websocket in self.clients:
            if websocket in self.client_ids:
                client_id = self.client_ids[websocket]
                if client_id in client_ids_to_notify:
                    try:
                        await websocket.send(message_json)
                        sent_count += 1
                    except Exception as e:
                        logger.error(f"Error enviando actualización recent_calls a cliente {client_id}: {e}")
                        disconnected_clients.append(websocket)
        
        # Limpiar clientes desconectados y sus suscripciones
        for websocket in disconnected_clients:
            self.clients.discard(websocket)
            if websocket in self.client_ids:
                client_id = self.client_ids[websocket]
                for campaign, clients in self.recent_calls_monitors.items():
                    clients.discard(client_id)
                del self.client_ids[websocket]

    async def register_client(self, websocket, path=None):
        self.clients.add(websocket)
        # Asignar ID único al cliente
        client_id = self.next_client_id
        self.next_client_id += 1
        self.client_ids[websocket] = client_id
        
        client_ip = websocket.remote_address[0] if websocket.remote_address else "unknown"
        logger.info(f"Cliente {client_id} conectado desde: {client_ip}")
        
        # Iniciar monitoreo de DB si es el primer cliente
        if len(self.clients) == 1:
            await self.start_db_monitoring()
        
        try:
            # Enviar estadísticas iniciales
            await websocket.send(json.dumps({
                "type": "stats_update",
                "data": self.current_stats
            }))
            
            # Escuchar mensajes del cliente
            async for message in websocket:
                await self.handle_client_message(websocket, message)
                
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Cliente {client_id} desconectado: {client_ip}")
        except Exception as e:
            logger.error(f"Error en conexión WebSocket: {e}")
        finally:
            self.clients.discard(websocket)
            # Limpiar datos del cliente
            if websocket in self.client_ids:
                client_id = self.client_ids[websocket]
                if client_id in self.monitored_campaigns:
                    del self.monitored_campaigns[client_id]
                del self.client_ids[websocket]
            logger.info(f"Cliente {client_id} desconectado: {client_ip}")

    # Remover la función _auto_push_database_updates ya que ahora se maneja con detección de cambios
    async def broadcast_stats(self, stats):
        # Si es multi-campaña, cada dict debe tener campaign_name y arrays
        if isinstance(stats, dict) and stats.get("type") == "multi_campaign_stats":
            # Asegura que cada campaña tenga arrays de números (para frontend)
            for c in stats["data"]:
                c.setdefault("ringing_numbers", [])
                c.setdefault("active_numbers", [])
                c.setdefault("answered_numbers", [])
                c.setdefault("failed_numbers", [])
                c.setdefault("busy_numbers", [])
                c.setdefault("no_answer_numbers", [])
            message = json.dumps({
                "type": "multi_campaign_stats",
                "data": stats["data"],
                "timestamp": datetime.now().isoformat()
            })
        else:
            self.current_stats.update(stats)
            self.current_stats["timestamp"] = datetime.now().isoformat()
            message = json.dumps({
                "type": "stats_update",
                "data": self.current_stats
            })
        await self._broadcast(message)
        # Ya no se hace auto-push aquí, se maneja por detección de cambios

    async def broadcast_event(self, event_type, data):
        """Envía eventos a todos los clientes conectados"""
        # Asegura que cada evento tenga el nombre de campaña si está disponible
        if isinstance(data, dict) and "campaign_name" not in data:
            if "campaign" in data:
                data["campaign_name"] = data["campaign"]

        # Si es campaign_finished, guarda los datos y cambia estado en BD
        if event_type == "campaign_finished" and "campaign_name" in data:
            campaign_name = data["campaign_name"]
            # Guardar snapshot de stats en memoria (por 24h)
            stats_snapshot = {
                "finished_at": datetime.now().isoformat(),
                "data": data.copy()
            }
            with self.finished_campaigns_lock:
                if campaign_name not in self.finished_campaigns:
                    self.finished_campaigns[campaign_name] = []
                self.finished_campaigns[campaign_name].append(stats_snapshot)
                # Set expiry for 24h
                from datetime import timedelta
                self.finished_campaigns_expiry[campaign_name] = datetime.now() + timedelta(hours=24)

        message = json.dumps({
            "type": event_type,
            "data": data,
            "timestamp": datetime.now().isoformat()
        })
        await self._broadcast(message)

    # Limpieza periódica de campanas finalizadas (llamar cada cierto tiempo)
    async def cleanup_finished_campaigns(self):
        while True:
            now = datetime.now()
            with self.finished_campaigns_lock:
                expired = [c for c, expiry in self.finished_campaigns_expiry.items() if expiry < now]
                for c in expired:
                    self.finished_campaigns.pop(c, None)
                    self.finished_campaigns_expiry.pop(c, None)
            await asyncio.sleep(3600)  # Revisa cada hora

    async def _broadcast(self, message):
        if not self.clients:
            logger.debug("No hay clientes conectados para enviar mensaje")
            return
        disconnected = []
        # Batch send para mejor performance
        tasks = []
        for client in self.clients:
            tasks.append(self._safe_send(client, message, disconnected))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for client in disconnected:
            self.clients.discard(client)
    
    async def _safe_send(self, client, message, disconnected):
        try:
            await client.send(message)
        except Exception:
            disconnected.append(client)

    async def start_server(self):
        # Inicia tanto WS como WSS en diferentes puertos
        import ssl, os
        ws_port = self.port
        wss_port = ws_port + 1  # Ejemplo: 8765 para WS, 8766 para WSS

        # WS (sin cifrado)
        logger.info(f"Iniciando servidor WebSocket (WS) en {self.host}:{ws_port}")
        try:
            ws_server = await websockets.serve(
                self.register_client,
                self.host,
                ws_port,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10,
                ssl=None
            )
            logger.info(f"Servidor WebSocket iniciado en ws://{self.host}:{ws_port}")
        except OSError as e:
            if "address already in use" in str(e).lower():
                logger.debug(f"WebSocket ya está corriendo en puerto {ws_port}")
                # Marcar como iniciado para evitar reintentos
                self._server = True
                self._server_ws = True
                return
            raise

        # WSS (con cifrado)
        cert_path = "/etc/asterisk/keys/asterisk-BestVoiper.crt"
        key_path = "/etc/asterisk/keys/asterisk-BestVoiper.key"
        ssl_context = None
        if os.path.exists(cert_path) and os.path.exists(key_path):
            try:
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                ssl_context.load_cert_chain(certfile=cert_path, keyfile=key_path)
                logger.info(f"Iniciando servidor WebSocket seguro (WSS) en {self.host}:{wss_port}")
                wss_server = await websockets.serve(
                    self.register_client,
                    self.host,
                    wss_port,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10,
                    ssl=ssl_context
                )
                logger.info(f"Servidor WebSocket iniciado en wss://{self.host}:{wss_port}")
            except Exception as e:
                logger.warning(f"No se pudo iniciar WSS: {e}")
        else:
            logger.info("Certificados SSL no encontrados, solo se inicia WS.")

        # Guarda ambos servidores para cierre/control
        self._server = ws_server  # Solo guarda uno para compatibilidad, pero ambos están activos
        self._server_ws = ws_server
        self._server_wss = wss_server if ssl_context else None

    async def ensure_running(self):
        if self._server is None:
            await self.start_server()

# Instancia global
ws_server = StatsWebSocketServer()

async def send_stats_to_websocket(stats_dict):
    await ws_server.ensure_running()
    await ws_server.broadcast_stats(stats_dict)

async def send_event_to_websocket(event_type, data):
    await ws_server.ensure_running()
    await ws_server.broadcast_event(event_type, data)

# Función para mantener el WebSocket activo con más frecuencia
async def keep_websocket_alive():
    """Mantiene el WebSocket activo enviando heartbeats periódicos"""
    while True:
        try:
            await ws_server.ensure_running()
            if len(ws_server.clients) > 0:
                await ws_server.broadcast_event("server_heartbeat", {
                    "status": "online",
                    "connected_clients": len(ws_server.clients),
                    "timestamp": datetime.now().isoformat(),
                    "mode": "push_realtime"
                })
            await asyncio.sleep(5)  # Heartbeat cada 5 segundos para updates más frecuentes
        except Exception as e:
            logger.error(f"Error en keep_websocket_alive: {e}")
            await asyncio.sleep(2)

if __name__ == "__main__":
    async def main():
        await ws_server.ensure_running()
        # Inicia el heartbeat del servidor
        heartbeat_task = asyncio.create_task(keep_websocket_alive())
        
        logger.info("Servidor WebSocket iniciado. Presiona Ctrl+C para detener.")
        try:
            while True:
                await asyncio.sleep(30)
                logger.info(f"Clientes activos: {len(ws_server.clients)}")
        except KeyboardInterrupt:
            logger.info("Deteniendo servidor WebSocket...")
        finally:
            heartbeat_task.cancel()
            if ws_server._server:
                ws_server._server.close()
                await ws_server._server.wait_closed()
    asyncio.run(main())
