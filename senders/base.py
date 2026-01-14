"""
Base Sender - Clase abstracta base para todos los senders
Define la interfaz común y funcionalidades compartidas
"""
import asyncio
import time
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Any
from datetime import datetime
from sqlalchemy import text

from shared_config import (
    get_logger, create_db_engine, CampaignType,
    REDIS_HOST, REDIS_PORT, REDIS_DB, CACHE_TTL
)

# Redis opcional
try:
    import redis
    redis_client = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
        decode_responses=True, socket_connect_timeout=5
    )
    redis_client.ping()
    REDIS_AVAILABLE = True
except:
    REDIS_AVAILABLE = False
    redis_client = None

# WebSocket opcional
try:
    from websocket_server import send_stats_to_websocket, send_event_to_websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False
    async def send_stats_to_websocket(*args, **kwargs): pass
    async def send_event_to_websocket(*args, **kwargs): pass


@dataclass
class SenderStats:
    """Estadísticas genéricas para cualquier tipo de sender"""
    campaign_name: str = ""
    campaign_type: str = ""
    
    # Contadores generales
    total_sent: int = 0
    total_delivered: int = 0
    total_failed: int = 0
    total_pending: int = 0
    
    # Sets para tracking únicos
    unique_sent: Set = field(default_factory=set)
    unique_delivered: Set = field(default_factory=set)
    unique_failed: Set = field(default_factory=set)
    
    # Activos
    active_items: Set = field(default_factory=set)
    
    # Métricas
    rate_current: float = 0.0  # Items por segundo actual
    rate_max: float = 0.0      # Rate máximo configurado
    
    # Timing
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    duration: float = 0.0
    
    # Datos adicionales específicos del sender
    extra_data: Dict = field(default_factory=dict)
    
    def calculate_duration(self) -> float:
        """Calcula la duración"""
        if self.start_time:
            self.duration = time.time() - self.start_time
        return self.duration
    
    def get_success_rate(self) -> float:
        """Calcula el porcentaje de éxito"""
        total = len(self.unique_sent)
        if total > 0:
            return (len(self.unique_delivered) / total) * 100
        return 0.0
    
    def to_dict(self) -> dict:
        """Serializa las estadísticas"""
        self.calculate_duration()
        
        duration_formatted = ""
        if self.duration > 0:
            minutes = int(self.duration // 60)
            seconds = int(self.duration % 60)
            duration_formatted = f"{minutes}m {seconds}s"
        
        return {
            "campaign_name": self.campaign_name,
            "campaign_type": self.campaign_type,
            
            "total_sent": self.total_sent,
            "unique_sent": len(self.unique_sent),
            "total_delivered": len(self.unique_delivered),
            "total_failed": len(self.unique_failed),
            "total_pending": self.total_pending,
            "active": len(self.active_items),
            
            "success_rate": round(self.get_success_rate(), 2),
            "rate_current": round(self.rate_current, 2),
            "rate_max": round(self.rate_max, 2),
            
            "duration": round(self.duration, 2),
            "duration_formatted": duration_formatted,
            
            **self.extra_data
        }


class BaseSender(ABC):
    """
    Clase base abstracta para todos los senders.
    Define la interfaz que deben implementar todos los tipos de campaña.
    """
    
    # Tipo de campaña que maneja este sender
    CAMPAIGN_TYPE: str = "Base"
    
    # Estados terminales (no reintentar)
    TERMINAL_STATES: tuple = ('C', 'X')
    
    # Estados que permiten reintento
    RETRY_STATES: tuple = ('F', 'E')
    
    def __init__(self, campaign_name: str, config: dict = None):
        self.campaign_name = campaign_name
        self.config = config or {}
        self.logger = get_logger(f"{self.CAMPAIGN_TYPE.lower()}_sender")
        self.engine = create_db_engine(pool_size=20, max_overflow=10)
        self.stats = SenderStats(
            campaign_name=campaign_name,
            campaign_type=self.CAMPAIGN_TYPE
        )
        self._running = False
        self._lock = threading.Lock()
        
        # Tracking de elementos activos
        self._active_ids: Set[str] = set()
        self._active_timestamps: Dict[str, float] = {}
    
    # =========================================================================
    # MÉTODOS ABSTRACTOS - Deben implementarse en cada sender
    # =========================================================================
    
    @abstractmethod
    async def send_single(self, item: dict) -> tuple:
        """
        Envía un solo elemento.
        
        Args:
            item: Diccionario con datos del elemento (telefono, email, etc)
        
        Returns:
            tuple: (item_id, success: bool, result: dict)
        """
        pass
    
    @abstractmethod
    async def send_batch(self, items: list) -> list:
        """
        Envía un lote de elementos.
        
        Args:
            items: Lista de diccionarios con datos
        
        Returns:
            list: Lista de (item_id, success, result)
        """
        pass
    
    @abstractmethod
    async def check_status(self, item_id: str) -> dict:
        """
        Verifica el estado de un elemento enviado.
        
        Args:
            item_id: Identificador del elemento
        
        Returns:
            dict: Estado actual
        """
        pass
    
    @abstractmethod
    async def initialize(self) -> bool:
        """
        Inicializa conexiones y recursos necesarios.
        
        Returns:
            bool: True si inicialización exitosa
        """
        pass
    
    @abstractmethod
    async def cleanup(self):
        """Limpia recursos al finalizar"""
        pass
    
    # =========================================================================
    # MÉTODOS COMUNES - Implementación base compartida
    # =========================================================================
    
    def get_campaign_config(self) -> dict:
        """Obtiene configuración de la campaña desde BD"""
        try:
            with self.engine.connect() as conn:
                # CPS se calcula automáticamente (no desde BD)
                # Usar queue_relationated en lugar de cola_destino
                # contexto no se usa - siempre es el nombre de la campaña
                result = conn.execute(text("""
                    SELECT reintentos, horarios, activo, api_config,
                           mensaje_template, queue_relationated
                    FROM campanas 
                    WHERE nombre = :nombre
                """), {"nombre": self.campaign_name}).fetchone()
                
                if result:
                    return {
                        "cps": 10,  # CPS automático
                        "max_retries": result[0] or 3,
                        "schedule": result[1],
                        "active": result[2],
                        "api_config": result[3],
                        "template": result[4],
                        "queue": result[5],
                    }
        except Exception as e:
            self.logger.error(f"Error obteniendo config: {e}")
        
        return {"cps": 10, "max_retries": 3, "active": "S"}
    
    async def get_pending_items(self, max_items: int = 100) -> list:
        """Obtiene elementos pendientes de la campaña"""
        try:
            config = self.get_campaign_config()
            max_retries = config.get("max_retries", 3)
            
            with self.engine.connect() as conn:
                # Query genérica - cada sender puede sobrescribir
                result = conn.execute(text(f"""
                    SELECT * FROM `{self.campaign_name}`
                    WHERE estado = 'pendiente'
                    OR (estado IN :retry_states 
                        AND (intentos IS NULL OR intentos < :max_retries))
                    LIMIT :limit
                """), {
                    "retry_states": self.RETRY_STATES,
                    "max_retries": max_retries,
                    "limit": max_items
                })
                
                columns = result.keys()
                items = []
                for row in result:
                    item = dict(zip(columns, row))
                    # Filtrar activos
                    item_id = str(item.get('telefono') or item.get('email') or item.get('id'))
                    if not self.is_active(item_id):
                        items.append(item)
                
                return items
        except Exception as e:
            self.logger.error(f"Error obteniendo pendientes: {e}")
            return []
    
    def is_active(self, item_id: str) -> bool:
        """Verifica si un elemento está activo"""
        with self._lock:
            if item_id in self._active_ids:
                ts = self._active_timestamps.get(item_id, 0)
                if time.time() - ts > 120:  # 2 minutos timeout
                    self._active_ids.discard(item_id)
                    self._active_timestamps.pop(item_id, None)
                    return False
                return True
            return False
    
    def register_active(self, item_id: str):
        """Registra un elemento como activo"""
        with self._lock:
            self._active_ids.add(item_id)
            self._active_timestamps[item_id] = time.time()
            self.stats.active_items.add(item_id)
    
    def release_active(self, item_id: str):
        """Libera un elemento del tracking"""
        with self._lock:
            self._active_ids.discard(item_id)
            self._active_timestamps.pop(item_id, None)
            self.stats.active_items.discard(item_id)
    
    async def update_item_status(self, item_id: str, status: str, extra: dict = None):
        """Actualiza el estado de un elemento en BD"""
        try:
            extra = extra or {}
            with self.engine.begin() as conn:
                # Determinar campo ID (telefono, email, etc)
                id_field = self._get_id_field()
                
                update_parts = ["estado = :status"]
                params = {"status": status, "item_id": item_id}
                
                if status in self.TERMINAL_STATES or status in self.RETRY_STATES:
                    update_parts.append("fecha_fin = NOW()")
                
                for key, value in extra.items():
                    update_parts.append(f"{key} = :{key}")
                    params[key] = value
                
                query = f"""
                    UPDATE `{self.campaign_name}`
                    SET {', '.join(update_parts)}
                    WHERE {id_field} = :item_id
                """
                conn.execute(text(query), params)
                
        except Exception as e:
            self.logger.error(f"Error actualizando estado: {e}")
    
    def _get_id_field(self) -> str:
        """Obtiene el campo ID según el tipo de campaña"""
        if self.CAMPAIGN_TYPE in ('Email',):
            return 'email'
        return 'telefono'
    
    async def is_campaign_active(self) -> bool:
        """Verifica si la campaña sigue activa"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT activo FROM campanas WHERE nombre = :nombre
                """), {"nombre": self.campaign_name}).fetchone()
                
                return result and result[0] == 'S'
        except:
            return False
    
    async def safe_send_websocket(self, event_type: str, data: dict):
        """Envío seguro al WebSocket"""
        if not WEBSOCKET_AVAILABLE:
            return
        try:
            await send_event_to_websocket(event_type, data)
        except Exception as e:
            self.logger.debug(f"WebSocket error: {e}")
    
    def update_rate_stats(self):
        """Actualiza estadísticas de rate"""
        if REDIS_AVAILABLE and redis_client:
            try:
                key = f"{self.CAMPAIGN_TYPE.lower()}:{self.campaign_name}:rate"
                current_time = time.time()
                # Contar eventos en el último segundo
                count = redis_client.zcount(key, current_time - 1, current_time)
                self.stats.rate_current = float(count)
            except:
                pass
    
    def log_stats(self):
        """Imprime estadísticas actuales"""
        stats = self.stats.to_dict()
        self.logger.info(
            f"📊 [{self.campaign_name}] "
            f"Enviados: {stats['unique_sent']} | "
            f"Entregados: {stats['total_delivered']} | "
            f"Fallidos: {stats['total_failed']} | "
            f"Rate: {stats['rate_current']}/s"
        )
    
    # =========================================================================
    # MÉTODO PRINCIPAL DE EJECUCIÓN
    # =========================================================================
    
    async def run(self, items: list = None) -> SenderStats:
        """
        Ejecuta el envío de la campaña.
        
        Args:
            items: Lista opcional de elementos. Si no se provee, obtiene pendientes.
        
        Returns:
            SenderStats: Estadísticas finales
        """
        self.stats.start_time = time.time()
        self._running = True
        
        self.logger.info(f"🚀 [{self.campaign_name}] Iniciando {self.CAMPAIGN_TYPE} sender...")
        
        # Inicializar
        if not await self.initialize():
            self.logger.error(f"❌ Error inicializando sender")
            return self.stats
        
        try:
            # Obtener configuración
            config = self.get_campaign_config()
            rate = config.get("cps", 10)
            self.stats.rate_max = rate
            
            # Obtener elementos si no se proveyeron
            if items is None:
                items = await self.get_pending_items(max_items=1000)
            
            if not items:
                self.logger.info(f"📋 [{self.campaign_name}] Sin elementos pendientes")
                return self.stats
            
            self.logger.info(f"📦 [{self.campaign_name}] {len(items)} elementos a procesar")
            self.stats.total_pending = len(items)
            
            # Procesar en batches
            batch_size = min(rate, 10)
            delay = 1.0 / rate if rate > 0 else 0.1
            
            for i in range(0, len(items), batch_size):
                # Verificar si debe continuar
                if not self._running or not await self.is_campaign_active():
                    self.logger.info(f"⏹️ [{self.campaign_name}] Detenido")
                    break
                
                batch = items[i:i + batch_size]
                
                # Enviar batch
                results = await self.send_batch(batch)
                
                # Procesar resultados
                for item_id, success, result in results:
                    self.stats.total_sent += 1
                    self.stats.unique_sent.add(item_id)
                    
                    if success:
                        self.stats.unique_delivered.add(item_id)
                    else:
                        self.stats.unique_failed.add(item_id)
                    
                    # WebSocket
                    await self.safe_send_websocket(f"{self.CAMPAIGN_TYPE.lower()}_sent", {
                        "campaign": self.campaign_name,
                        "item_id": item_id,
                        "success": success,
                        "result": result
                    })
                
                # Rate limiting
                await asyncio.sleep(delay * batch_size)
                
                # Log periódico
                if (i // batch_size) % 10 == 0:
                    self.log_stats()
            
        except Exception as e:
            self.logger.error(f"❌ Error en sender: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
        finally:
            await self.cleanup()
            self._running = False
        
        # Estadísticas finales
        self.stats.end_time = time.time()
        self.stats.calculate_duration()
        
        self.logger.info(f"")
        self.logger.info(f"{'='*60}")
        self.logger.info(f"✅ [{self.campaign_name}] {self.CAMPAIGN_TYPE} FINALIZADO")
        self.logger.info(f"   📤 Enviados: {len(self.stats.unique_sent)}")
        self.logger.info(f"   ✅ Entregados: {len(self.stats.unique_delivered)}")
        self.logger.info(f"   ❌ Fallidos: {len(self.stats.unique_failed)}")
        self.logger.info(f"   📈 Tasa éxito: {self.stats.get_success_rate():.1f}%")
        self.logger.info(f"   ⏱️  Duración: {self.stats.duration:.2f}s")
        self.logger.info(f"{'='*60}")
        
        return self.stats
    
    def stop(self):
        """Detiene el sender"""
        self._running = False
        self.logger.info(f"⏹️ [{self.campaign_name}] Deteniendo...")
