#!/usr/bin/env python3
"""
Call Sender - Módulo independiente para envío de llamadas optimizado
Maneja el envío de llamadas con máximo rendimiento y CPS controlado

Puede ejecutarse como proceso independiente:
    python call_sender.py --campaign NOMBRE_CAMPANA

O importarse como módulo:
    from call_sender import CallSender, send_campaign_calls
"""

import asyncio
import time
import logging
import sys
import argparse
import signal
import uuid as uuid_lib
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Callable, Any
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
from collections import deque
import threading

# SQLAlchemy
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import ProgrammingError, OperationalError

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("CallSender")

# Configuración
FREESWITCH_HOST = "127.0.0.1"
FREESWITCH_PORT = 8021
FREESWITCH_PASSWORD = "1Pl}0F~~801l"
GATEWAY = "gw_pstn"
DB_URL = "mysql+pymysql://consultas:consultas@localhost/masivos"

# Importar ESL
try:
    import ESL
except ImportError:
    logger.error("ESL bindings no instalados. Instala los bindings oficiales de FreeSWITCH.")
    sys.exit(1)

# Integración con Redis
try:
    from redis_manager import RedisCallManager
    redis_manager = RedisCallManager()
    REDIS_AVAILABLE = redis_manager.ping()
    if REDIS_AVAILABLE:
        logger.info("✅ Redis disponible")
    else:
        logger.warning("⚠️ Redis servidor no responde (¿está ejecutándose?)")
except ImportError as e:
    REDIS_AVAILABLE = False
    redis_manager = None
    if "redis" in str(e).lower():
        logger.warning("⚠️ Paquete 'redis' de Python no instalado (pip install redis)")
    else:
        logger.warning(f"⚠️ Error importando redis_manager: {e}")
except Exception as e:
    REDIS_AVAILABLE = False
    redis_manager = None
    logger.warning(f"⚠️ Error conectando Redis: {e}")


@dataclass
class CallRequest:
    """Representa una solicitud de llamada"""
    numero: str
    campaign_name: str
    uuid: str = field(default_factory=lambda: "")
    amd_type: str = "PRO"
    destino: str = "9999"
    priority: int = 0
    created_at: float = field(default_factory=time.time)
    
    def __post_init__(self):
        if not self.uuid:
            self.uuid = f"{self.campaign_name}_{self.numero}_{int(time.time()*1000000)}"


@dataclass  
class CallResult:
    """Resultado de una llamada enviada"""
    numero: str
    uuid: str
    success: bool
    error_message: str = ""
    sent_at: float = field(default_factory=time.time)


@dataclass
class CampaignStats:
    """Estadísticas de una campaña"""
    campaign_name: str
    calls_sent: int = 0
    calls_success: int = 0
    calls_failed: int = 0
    start_time: float = field(default_factory=time.time)
    last_send_time: float = 0
    cps_current: float = 0.0
    cps_max: float = 0.0
    
    def update_cps(self, window: float = 3.0):
        """Actualiza el CPS actual basado en la ventana de tiempo"""
        if self.calls_sent > 0:
            elapsed = time.time() - self.start_time
            if elapsed > 0:
                self.cps_current = self.calls_sent / elapsed
                if self.cps_current > self.cps_max:
                    self.cps_max = self.cps_current
                    
    def to_dict(self) -> Dict:
        return {
            "campaign_name": self.campaign_name,
            "calls_sent": self.calls_sent,
            "calls_success": self.calls_success,
            "calls_failed": self.calls_failed,
            "cps_current": round(self.cps_current, 2),
            "cps_max": round(self.cps_max, 2),
            "duration": round(time.time() - self.start_time, 2)
        }


class ESLConnectionPool:
    """Pool de conexiones ESL para envío paralelo"""
    
    def __init__(self, pool_size: int = 5):
        self.pool_size = pool_size
        self.connections: List[ESL.ESLconnection] = []
        self.available: asyncio.Queue = None
        self._lock = asyncio.Lock()
        self._initialized = False
        
    async def initialize(self):
        """Inicializa el pool de conexiones"""
        if self._initialized:
            return
            
        self.available = asyncio.Queue()
        
        for i in range(self.pool_size):
            conn = self._create_connection()
            if conn:
                await self.available.put(conn)
                self.connections.append(conn)
                
        self._initialized = True
        logger.info(f"✅ Pool ESL inicializado con {self.available.qsize()} conexiones")
        
    def _create_connection(self) -> Optional[ESL.ESLconnection]:
        """Crea una nueva conexión ESL"""
        try:
            conn = ESL.ESLconnection(FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD)
            if conn.connected():
                return conn
            else:
                logger.warning("⚠️ No se pudo crear conexión ESL")
                return None
        except Exception as e:
            logger.error(f"❌ Error creando conexión ESL: {e}")
            return None
            
    async def get_connection(self) -> Optional[ESL.ESLconnection]:
        """Obtiene una conexión del pool"""
        try:
            conn = await asyncio.wait_for(self.available.get(), timeout=5.0)
            if not conn.connected():
                # Reconectar si está desconectada
                conn = self._create_connection()
            return conn
        except asyncio.TimeoutError:
            logger.warning("⚠️ Timeout obteniendo conexión ESL")
            return self._create_connection()
        except Exception as e:
            logger.error(f"❌ Error obteniendo conexión: {e}")
            return None
            
    async def return_connection(self, conn: ESL.ESLconnection):
        """Devuelve una conexión al pool"""
        if conn and conn.connected():
            await self.available.put(conn)
        else:
            # Si está desconectada, crear una nueva
            new_conn = self._create_connection()
            if new_conn:
                await self.available.put(new_conn)
                
    async def close_all(self):
        """Cierra todas las conexiones"""
        for conn in self.connections:
            try:
                if conn.connected():
                    conn.disconnect()
            except:
                pass
        self.connections.clear()
        self._initialized = False
        logger.info("🔌 Pool ESL cerrado")


class CallSenderWorker:
    """Worker para envío de llamadas en paralelo"""
    
    def __init__(self, worker_id: int, connection_pool: ESLConnectionPool,
                 request_queue: asyncio.Queue, result_callback: Callable):
        self.worker_id = worker_id
        self.connection_pool = connection_pool
        self.request_queue = request_queue
        self.result_callback = result_callback
        self.running = False
        self._sent_count = 0
        self._error_count = 0
        
    async def send_call(self, request: CallRequest) -> CallResult:
        """Envía una llamada individual"""
        conn = await self.connection_pool.get_connection()
        if not conn:
            logger.warning(f"📵 [{request.campaign_name}] Sin conexión ESL para {request.numero}")
            return CallResult(
                numero=request.numero,
                uuid=request.uuid,
                success=False,
                error_message="No hay conexión ESL disponible"
            )
            
        try:
            # Construir comando originate
            logger.debug(f"📤 [{request.campaign_name}] Enviando llamada a {request.numero} (UUID: {request.uuid})")
            if request.amd_type and request.amd_type.upper() == "FREE":
                originate_str = (
                    f"bgapi originate "
                    f"{{ignore_early_media=false,"
                    f"origination_uuid={request.uuid},"
                    f"campaign_name='{request.campaign_name}',"
                    f"origination_caller_id_number='{request.numero}',"
                    f"execute_on_answer='transfer {request.destino} XML {request.campaign_name}'}}"
                    f"sofia/gateway/{GATEWAY}/{request.numero} &park()"
                )
            else:
                # AMD PRO con transferencia automática
                originate_str = (
                    f"bgapi originate "
                    f"{{ignore_early_media=false,"
                    f"origination_uuid={request.uuid},"
                    f"campaign_name='{request.campaign_name}',"
                    f"origination_caller_id_number='{request.numero}',"
                    f"execute_on_answer='transfer {request.destino} XML {request.campaign_name}'}}"
                    f"sofia/gateway/{GATEWAY}/{request.numero} 2222 XML DETECT_AMD_PRO"
                )
                
            # Ejecutar en thread para no bloquear
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: conn.api(originate_str)
            )
            
            success = False
            error_msg = ""
            
            if response:
                body = response.getBody()
                success = "+OK" in body or "Job-UUID" in body
                if not success:
                    error_msg = body[:100]
                    logger.warning(f"⚠️ [{request.campaign_name}] Llamada rechazada {request.numero}: {error_msg}")
                else:
                    logger.info(f"📞 [{request.campaign_name}] Llamada enviada: {request.numero} | UUID: {request.uuid}")
            else:
                error_msg = "Sin respuesta de FreeSWITCH"
                logger.error(f"❌ [{request.campaign_name}] Sin respuesta FS para {request.numero}")
                
            return CallResult(
                numero=request.numero,
                uuid=request.uuid,
                success=success,
                error_message=error_msg
            )
            
        except Exception as e:
            logger.error(f"❌ Worker #{self.worker_id}: Error enviando {request.numero}: {e}")
            return CallResult(
                numero=request.numero,
                uuid=request.uuid,
                success=False,
                error_message=str(e)
            )
        finally:
            await self.connection_pool.return_connection(conn)
            
    async def run(self):
        """Loop principal del worker"""
        self.running = True
        logger.info(f"📞 Worker #{self.worker_id} iniciado")
        
        while self.running:
            try:
                # Obtener request de la cola
                try:
                    request = await asyncio.wait_for(
                        self.request_queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                    
                if request is None:  # Señal de terminar
                    break
                    
                # Enviar llamada
                result = await self.send_call(request)
                
                # Callback con resultado
                if self.result_callback:
                    await self.result_callback(result)
                    
                if result.success:
                    self._sent_count += 1
                else:
                    self._error_count += 1
                    
                self.request_queue.task_done()
                
            except Exception as e:
                logger.error(f"❌ Worker #{self.worker_id} error: {e}")
                await asyncio.sleep(0.1)
                
        logger.info(f"🛑 Worker #{self.worker_id} detenido (enviados: {self._sent_count}, errores: {self._error_count})")
        
    def stop(self):
        """Detiene el worker"""
        self.running = False


class CallSender:
    """
    Gestor principal de envío de llamadas
    Coordina múltiples workers para envío paralelo optimizado
    """
    
    def __init__(self, num_workers: int = 10, max_concurrent_calls: int = 1000,
                 cps_limit: int = 30):
        self.num_workers = num_workers
        self.max_concurrent_calls = max_concurrent_calls
        self.cps_limit = cps_limit
        
        # Componentes
        self.connection_pool = ESLConnectionPool(pool_size=num_workers)
        self.request_queue = asyncio.Queue(maxsize=10000)
        self.workers: List[CallSenderWorker] = []
        
        # Estado
        self.active_uuids: Set[str] = set()
        self.uuid_timestamps: Dict[str, float] = {}
        self.campaign_stats: Dict[str, CampaignStats] = {}
        
        # Control
        self.running = False
        self._lock = asyncio.Lock()
        self._cps_tokens = asyncio.Semaphore(cps_limit)
        self._concurrent_limiter = asyncio.Semaphore(max_concurrent_calls)
        
        # Base de datos
        self.engine = create_engine(
            DB_URL,
            pool_size=20,
            max_overflow=10,
            pool_recycle=1800,
            pool_pre_ping=True,
            poolclass=QueuePool
        )
        
        # Callbacks
        self.on_call_sent: Optional[Callable] = None
        self.on_call_result: Optional[Callable] = None
        
    async def initialize(self):
        """Inicializa el sender"""
        await self.connection_pool.initialize()
        
        # Crear workers
        for i in range(self.num_workers):
            worker = CallSenderWorker(
                worker_id=i,
                connection_pool=self.connection_pool,
                request_queue=self.request_queue,
                result_callback=self._handle_result
            )
            self.workers.append(worker)
            
        logger.info(f"✅ CallSender inicializado con {self.num_workers} workers")
        
    async def _handle_result(self, result: CallResult):
        """Maneja el resultado de una llamada enviada"""
        async with self._lock:
            if result.success:
                self.active_uuids.add(result.uuid)
                self.uuid_timestamps[result.uuid] = time.time()
            else:
                # Liberar si falló
                self.active_uuids.discard(result.uuid)
                self.uuid_timestamps.pop(result.uuid, None)
                
        # Callback externo
        if self.on_call_result:
            await self.on_call_result(result)
            
    async def send_call(self, numero: str, campaign_name: str, 
                       amd_type: str = "PRO", destino: str = "9999") -> CallResult:
        """Envía una llamada individual directamente"""
        # Verificar límite de concurrencia
        async with self._concurrent_limiter:
            request = CallRequest(
                numero=numero,
                campaign_name=campaign_name,
                amd_type=amd_type,
                destino=destino
            )
            
            # Registrar UUID
            async with self._lock:
                self.active_uuids.add(request.uuid)
                self.uuid_timestamps[request.uuid] = time.time()
            
            # Enviar llamada directamente (sin usar cola de workers)
            result = await self._send_call_direct(request)
            
            # Manejar resultado
            await self._handle_result(result)
            
            # Actualizar stats
            if campaign_name not in self.campaign_stats:
                self.campaign_stats[campaign_name] = CampaignStats(campaign_name=campaign_name)
            
            if result.success:
                self.campaign_stats[campaign_name].calls_sent += 1
            else:
                self.campaign_stats[campaign_name].calls_failed += 1
            
            return result
    
    async def _send_call_direct(self, request: CallRequest) -> CallResult:
        """Envía una llamada directamente usando el pool de conexiones"""
        conn = await self.connection_pool.get_connection()
        if not conn:
            logger.warning(f"📵 [{request.campaign_name}] Sin conexión ESL para {request.numero}")
            return CallResult(
                numero=request.numero,
                uuid=request.uuid,
                success=False,
                error_message="No hay conexión ESL disponible"
            )
            
        try:
            # Construir comando originate
            logger.debug(f"📤 [{request.campaign_name}] Preparando originate para {request.numero}")
            if request.amd_type and request.amd_type.upper() == "FREE":
                originate_str = (
                    f"bgapi originate "
                    f"{{ignore_early_media=false,"
                    f"origination_uuid={request.uuid},"
                    f"campaign_name='{request.campaign_name}',"
                    f"origination_caller_id_number='{request.numero}',"
                    f"execute_on_answer='transfer {request.destino} XML {request.campaign_name}'}}"
                    f"sofia/gateway/{GATEWAY}/{request.numero} &park()"
                )
            else:
                # AMD PRO con transferencia automática
                originate_str = (
                    f"bgapi originate "
                    f"{{ignore_early_media=false,"
                    f"origination_uuid={request.uuid},"
                    f"campaign_name='{request.campaign_name}',"
                    f"origination_caller_id_number='{request.numero}',"
                    f"execute_on_answer='transfer {request.destino} XML {request.campaign_name}'}}"
                    f"sofia/gateway/{GATEWAY}/{request.numero} 2222 XML DETECT_AMD_PRO"
                )
                
            # Ejecutar en thread para no bloquear
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: conn.api(originate_str)
            )
            
            success = False
            error_msg = ""
            
            if response:
                body = response.getBody()
                success = "+OK" in body or "Job-UUID" in body
                if not success:
                    error_msg = body[:100] if body else "Respuesta vacía"
                    logger.warning(f"⚠️ [{request.campaign_name}] Llamada rechazada {request.numero}: {error_msg}")
                else:
                    logger.info(f"📞 [{request.campaign_name}] Llamada enviada: {request.numero} | UUID: {request.uuid} | AMD: {request.amd_type}")
            else:
                error_msg = "Sin respuesta de FreeSWITCH"
                logger.error(f"❌ [{request.campaign_name}] Sin respuesta FS para {request.numero}")
                
            return CallResult(
                numero=request.numero,
                uuid=request.uuid,
                success=success,
                error_message=error_msg
            )
            
        except Exception as e:
            logger.error(f"❌ Error enviando {request.numero}: {e}")
            return CallResult(
                numero=request.numero,
                uuid=request.uuid,
                success=False,
                error_message=str(e)
            )
        finally:
            await self.connection_pool.return_connection(conn)
            
    async def send_batch(self, numbers: List[str], campaign_name: str,
                        amd_type: str = "PRO", destino: str = "9999",
                        cps: int = 10) -> List[CallResult]:
        """
        Envía un batch de llamadas con CPS controlado
        
        Args:
            numbers: Lista de números a llamar
            campaign_name: Nombre de la campaña
            amd_type: Tipo de AMD (PRO o FREE)
            destino: Destino de transferencia
            cps: Calls per second
            
        Returns:
            Lista de resultados
        """
        logger.info(f"📥 send_batch LLAMADO: campaign={campaign_name}, numbers={len(numbers)}, cps={cps}")
        logger.info(f"🔧 Estado: running={self.running}, active_uuids={len(self.active_uuids)}, max_concurrent={self.max_concurrent_calls}")
        
        results = []
        delay = 1.0 / cps if cps > 0 else 0.1
        logger.info(f"⏱️ Delay calculado: {delay}s entre llamadas")
        
        # Inicializar stats de campaña
        if campaign_name not in self.campaign_stats:
            self.campaign_stats[campaign_name] = CampaignStats(campaign_name=campaign_name)
        stats = self.campaign_stats[campaign_name]
        
        logger.info(f"📞 [{campaign_name}] Iniciando envío de {len(numbers)} llamadas a {cps} CPS")
        
        for i, numero in enumerate(numbers):
            if not self.running:
                logger.warning(f"⚠️ [{campaign_name}] Loop detenido porque running=False")
                break
            
            # Log para primeras 3 llamadas
            if i < 3:
                logger.info(f"📞 [{campaign_name}] Procesando #{i+1}: {numero}")
                
            # Control de CPS
            await asyncio.sleep(delay)
            
            # Verificar límite de concurrencia
            while len(self.active_uuids) >= self.max_concurrent_calls:
                await asyncio.sleep(0.1)
                await self._cleanup_stale_uuids()
                
            # Enviar
            result = await self.send_call(numero, campaign_name, amd_type, destino)
            results.append(result)
            
            # Log individual de envío
            if result.success:
                logger.debug(f"✅ [{campaign_name}] #{i+1}/{len(numbers)} - {numero} enviado OK")
            else:
                logger.warning(f"❌ [{campaign_name}] #{i+1}/{len(numbers)} - {numero} falló: {result.error_message}")
            
            # Registrar en Redis
            if REDIS_AVAILABLE and redis_manager:
                try:
                    redis_manager.register_call_sent(campaign_name)
                except:
                    pass
                    
            # Log progreso cada 100 llamadas
            if (i + 1) % 100 == 0:
                stats.update_cps()
                logger.info(
                    f"📊 [{campaign_name}] Progreso: {i+1}/{len(numbers)} "
                    f"(CPS: {stats.cps_current:.2f})"
                )
                
        stats.update_cps()
        logger.info(f"✅ [{campaign_name}] Batch completado: {len(results)} llamadas enviadas")
        
        return results
        
    async def _cleanup_stale_uuids(self, max_age: float = 60.0):
        """Limpia UUIDs obsoletos"""
        current_time = time.time()
        stale = []
        
        async with self._lock:
            for uuid, timestamp in list(self.uuid_timestamps.items()):
                if current_time - timestamp > max_age:
                    stale.append(uuid)
                    
            for uuid in stale:
                self.active_uuids.discard(uuid)
                self.uuid_timestamps.pop(uuid, None)
                
        if stale:
            logger.debug(f"🧹 Limpiados {len(stale)} UUIDs obsoletos")
            
    def release_uuid(self, uuid: str):
        """Libera un UUID del tracking"""
        self.active_uuids.discard(uuid)
        self.uuid_timestamps.pop(uuid, None)
        
    async def start(self):
        """Inicia el sender y sus workers"""
        self.running = True
        
        # Inicializar pool
        await self.initialize()
        
        # Iniciar workers
        tasks = []
        for worker in self.workers:
            tasks.append(asyncio.create_task(worker.run()))
            
        # Tarea de limpieza
        async def cleanup_loop():
            while self.running:
                await asyncio.sleep(30)
                await self._cleanup_stale_uuids()
                
        tasks.append(asyncio.create_task(cleanup_loop()))
        
        logger.info("🚀 CallSender iniciado")
        
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
            
    async def stop(self):
        """Detiene el sender"""
        logger.info("🛑 Deteniendo CallSender...")
        self.running = False
        
        # Enviar señal de terminar a workers
        for _ in self.workers:
            await self.request_queue.put(None)
            
        # Detener workers
        for worker in self.workers:
            worker.stop()
            
        # Cerrar pool
        await self.connection_pool.close_all()
        
        logger.info("✅ CallSender detenido")
        
    def get_stats(self) -> Dict:
        """Obtiene estadísticas del sender"""
        return {
            "active_uuids": len(self.active_uuids),
            "max_concurrent": self.max_concurrent_calls,
            "queue_size": self.request_queue.qsize(),
            "campaigns": {
                name: stats.to_dict() 
                for name, stats in self.campaign_stats.items()
            }
        }


# ============================================================================
# API para integración con check_calls.py
# ============================================================================

_sender_instance: Optional[CallSender] = None
_sender_lock = asyncio.Lock()

async def get_sender(num_workers: int = 10, max_concurrent: int = 1000,
                     cps_limit: int = 30) -> CallSender:
    """Obtiene o crea la instancia del sender"""
    global _sender_instance
    
    async with _sender_lock:
        if _sender_instance is None:
            _sender_instance = CallSender(
                num_workers=num_workers,
                max_concurrent_calls=max_concurrent,
                cps_limit=cps_limit
            )
            await _sender_instance.initialize()
            _sender_instance.running = True  # Activar para que send_batch funcione
            
    return _sender_instance


async def send_campaign_calls(numbers: List[str], campaign_name: str,
                             cps: int = 10, amd_type: str = "PRO",
                             destino: str = "9999",
                             on_result: Callable = None) -> List[CallResult]:
    """
    API simplificada para enviar llamadas de una campaña
    Compatible con la interfaz de check_calls.py
    
    Args:
        numbers: Lista de números a llamar
        campaign_name: Nombre de la campaña
        cps: Calls per second
        amd_type: Tipo de AMD
        destino: Destino de transferencia
        on_result: Callback para cada resultado
        
    Returns:
        Lista de resultados
    """
    logger.info(f"📥 send_campaign_calls LLAMADO: campaign={campaign_name}, numbers={len(numbers)}, cps={cps}")
    
    try:
        sender = await get_sender()
        logger.info(f"🔧 Sender obtenido: running={sender.running}, pool_initialized={sender.connection_pool._initialized}")
        
        if on_result:
            sender.on_call_result = on_result
            
        logger.info(f"🚀 Llamando a send_batch...")
        results = await sender.send_batch(
            numbers=numbers,
            campaign_name=campaign_name,
            amd_type=amd_type,
            destino=destino,
            cps=cps
        )
        logger.info(f"✅ send_batch retornó {len(results)} resultados")
        return results
        
    except Exception as e:
        logger.error(f"❌ Error en send_campaign_calls: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []


async def release_call_uuid(uuid: str):
    """Libera un UUID del tracking global"""
    global _sender_instance
    if _sender_instance:
        _sender_instance.release_uuid(uuid)


def get_active_uuids_count() -> int:
    """Retorna el número de UUIDs activos"""
    global _sender_instance
    if _sender_instance:
        return len(_sender_instance.active_uuids)
    return 0


# ============================================================================
# Ejecución como proceso independiente
# ============================================================================

async def process_campaign(campaign_name: str, cps: int = 10, 
                          amd_type: str = "PRO"):
    """Procesa una campaña completa"""
    sender = await get_sender()
    
    # Obtener números pendientes
    engine = create_engine(DB_URL, pool_pre_ping=True)
    
    with engine.connect() as conn:
        # Obtener max_intentos
        result = conn.execute(text(
            "SELECT reintentos FROM campanas WHERE nombre = :nombre"
        ), {"nombre": campaign_name}).fetchone()
        max_intentos = result[0] if result else 3
        
        # Obtener números pendientes
        result = conn.execute(text(f"""
            SELECT telefono FROM `{campaign_name}`
            WHERE (estado = 'pendiente')
               OR (estado IN ('N', 'U', 'E', 'R', 'I', 'X', 'T') AND intentos < :max_intentos)
        """), {"max_intentos": max_intentos})
        numbers = [row[0] for row in result]
        
    if not numbers:
        logger.info(f"📋 [{campaign_name}] No hay números pendientes")
        return
        
    logger.info(f"📋 [{campaign_name}] {len(numbers)} números pendientes")
    
    # Enviar llamadas
    results = await send_campaign_calls(
        numbers=numbers,
        campaign_name=campaign_name,
        cps=cps,
        amd_type=amd_type
    )
    
    # Resumen
    success = sum(1 for r in results if r.success)
    failed = len(results) - success
    
    logger.info(f"""
{'='*60}
✅ [{campaign_name}] ENVÍO COMPLETADO
   📞 Total: {len(results)}
   ✔️  Exitosos: {success}
   ❌ Fallidos: {failed}
{'='*60}
""")


async def main():
    """Función principal para ejecución independiente"""
    parser = argparse.ArgumentParser(description="Call Sender")
    parser.add_argument("--campaign", type=str, help="Nombre de la campaña")
    parser.add_argument("--cps", type=int, default=10, help="Calls per second")
    parser.add_argument("--workers", type=int, default=10, help="Número de workers")
    parser.add_argument("--max-concurrent", type=int, default=1000, help="Máximo de llamadas concurrentes")
    parser.add_argument("--amd", type=str, default="PRO", choices=["PRO", "FREE"], help="Tipo de AMD")
    parser.add_argument("--daemon", action="store_true", help="Ejecutar como daemon")
    args = parser.parse_args()
    
    global _sender_instance
    
    # Crear sender
    sender = await get_sender(
        num_workers=args.workers,
        max_concurrent=args.max_concurrent,
        cps_limit=args.cps
    )
    _sender_instance = sender
    
    # Manejar señales
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        logger.info("📡 Señal de terminación recibida")
        asyncio.create_task(sender.stop())
        
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass
            
    try:
        if args.daemon:
            # Modo daemon: esperar tareas
            logger.info("🚀 CallSender ejecutándose en modo daemon")
            await sender.start()
        elif args.campaign:
            # Procesar campaña específica
            await process_campaign(
                campaign_name=args.campaign,
                cps=args.cps,
                amd_type=args.amd
            )
        else:
            print("Uso: python call_sender.py --campaign NOMBRE [--cps 10] [--amd PRO|FREE]")
            print("     python call_sender.py --daemon")
            
    except KeyboardInterrupt:
        await sender.stop()
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        await sender.stop()
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️ CallSender detenido")
    except Exception as e:
        logger.error(f"❌ Error fatal: {e}")
        sys.exit(1)
