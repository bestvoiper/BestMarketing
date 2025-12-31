"""
Redis Manager para gestionar estados de llamadas y métricas CPS en tiempo real
"""
import redis
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional
from sqlalchemy import create_engine, text, Table, Column, Integer, String, Float, DateTime, MetaData
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

class RedisCallManager:
    def __init__(self, host='localhost', port=6379, db=0, mysql_url=None):
        """
        Inicializa el gestor de Redis para llamadas
        
        Args:
            host: Host de Redis
            port: Puerto de Redis
            db: Base de datos de Redis
            mysql_url: URL de conexión a MySQL para sincronización
        """
        self.redis_client = redis.Redis(
            host=host, 
            port=port, 
            db=db, 
            decode_responses=True
        )
        self.cps_window = 3  # Ventana de 3 segundos para CPS preciso y estable
        
        # Configurar conexión a MySQL para sincronización
        self.mysql_url = mysql_url or "mysql+pymysql://consultas:consultas@localhost/masivos"
        self.mysql_engine = create_engine(self.mysql_url, pool_pre_ping=True)
        self.metadata = MetaData()
        
    def _get_campaign_key(self, campaign_name: str, key_type: str) -> str:
        """Genera la clave de Redis para una campaña específica"""
        return f"campaign:{campaign_name}:{key_type}"
    
    def _get_global_key(self, key_type: str) -> str:
        """Genera la clave de Redis global"""
        return f"global:{key_type}"
    
    def set_call_state(self, campaign_name: str, numero: str, uuid: str, estado: str, 
                       metadata: Optional[Dict] = None):
        """
        Almacena el estado de una llamada en Redis
        
        Args:
            campaign_name: Nombre de la campaña
            numero: Número de teléfono
            uuid: UUID de la llamada
            estado: Estado de la llamada (pendiente, P, S, C, E, O, N, U, etc.)
            metadata: Información adicional de la llamada
        """
        try:
            call_data = {
                "numero": numero,
                "uuid": uuid,
                "estado": estado,
                "timestamp": datetime.now().isoformat(),
                "campaign_name": campaign_name
            }
            
            if metadata:
                call_data.update(metadata)
            
            # Guardar en hash de llamadas activas por UUID
            key = self._get_campaign_key(campaign_name, "calls")
            self.redis_client.hset(key, uuid, json.dumps(call_data))
            
            # Guardar en hash global de llamadas activas
            global_key = self._get_global_key("active_calls")
            self.redis_client.hset(global_key, uuid, json.dumps(call_data))
            
            # Actualizar contador de estado
            state_counter_key = self._get_campaign_key(campaign_name, f"state:{estado}")
            self.redis_client.incr(state_counter_key)
            
            # Si es estado terminal, remover de activas después de 5 minutos
            if self._is_terminal_state(estado):
                self.redis_client.expire(f"{key}:{uuid}", 300)
                
            logger.debug(f"Estado guardado en Redis: {campaign_name} - {numero} ({uuid}) -> {estado}")
            
        except Exception as e:
            logger.error(f"Error guardando estado en Redis: {e}")
    
    def _is_terminal_state(self, estado: str) -> bool:
        """Determina si un estado es terminal"""
        terminal_states = ['C', 'E', 'O', 'N', 'U', 'R', 'I', 'X', 'T', 'M']
        return estado.upper() in terminal_states
    
    def get_call_state(self, campaign_name: str, uuid: str) -> Optional[Dict]:
        """
        Obtiene el estado de una llamada específica
        
        Args:
            campaign_name: Nombre de la campaña
            uuid: UUID de la llamada
            
        Returns:
            Diccionario con los datos de la llamada o None
        """
        try:
            key = self._get_campaign_key(campaign_name, "calls")
            data = self.redis_client.hget(key, uuid)
            
            if data:
                return json.loads(data)
            return None
            
        except Exception as e:
            logger.error(f"Error obteniendo estado de Redis: {e}")
            return None
    
    def get_all_calls(self, campaign_name: str) -> List[Dict]:
        """
        Obtiene todas las llamadas de una campaña
        
        Args:
            campaign_name: Nombre de la campaña
            
        Returns:
            Lista de llamadas
        """
        try:
            key = self._get_campaign_key(campaign_name, "calls")
            calls_data = self.redis_client.hgetall(key)
            
            return [json.loads(call_json) for call_json in calls_data.values()]
            
        except Exception as e:
            logger.error(f"Error obteniendo llamadas de Redis: {e}")
            return []
    
    def remove_call(self, campaign_name: str, uuid: str):
        """
        Elimina una llamada del registro de Redis
        
        Args:
            campaign_name: Nombre de la campaña
            uuid: UUID de la llamada
        """
        try:
            key = self._get_campaign_key(campaign_name, "calls")
            self.redis_client.hdel(key, uuid)
            
            global_key = self._get_global_key("active_calls")
            self.redis_client.hdel(global_key, uuid)
            
            logger.debug(f"Llamada removida de Redis: {campaign_name} - {uuid}")
            
        except Exception as e:
            logger.error(f"Error removiendo llamada de Redis: {e}")
    
    def register_call_sent(self, campaign_name: str):
        """
        Registra una llamada enviada para cálculo de CPS con precisión de microsegundos
        
        Args:
            campaign_name: Nombre de la campaña
        """
        try:
            # Usar timestamp con precisión de microsegundos
            current_time = time.time()
            
            # Usar pipeline para operaciones atómicas y más rápidas
            pipe = self.redis_client.pipeline()
            
            # Agregar timestamp a lista ordenada para la campaña
            key = self._get_campaign_key(campaign_name, "cps_calls")
            pipe.zadd(key, {str(current_time): current_time})
            
            # Agregar a lista global
            global_key = self._get_global_key("cps_calls")
            pipe.zadd(global_key, {f"{campaign_name}:{current_time}": current_time})
            
            # Limpiar llamadas antiguas (fuera de ventana de 60 segundos)
            min_time = current_time - 60
            pipe.zremrangebyscore(key, 0, min_time)
            pipe.zremrangebyscore(global_key, 0, min_time)
            
            # Ejecutar todas las operaciones en un solo round-trip
            pipe.execute()
            
        except Exception as e:
            logger.error(f"Error registrando llamada enviada en Redis: {e}")
    
    def register_calls_sent_batch(self, campaign_name: str, count: int):
        """
        Registra múltiples llamadas enviadas de una vez (batch)
        MUCHO más rápido que llamar a register_call_sent múltiples veces
        
        Args:
            campaign_name: Nombre de la campaña
            count: Número de llamadas a registrar
        """
        if count <= 0:
            return
        
        try:
            current_time = time.time()
            
            # Usar pipeline para batch insert
            pipe = self.redis_client.pipeline()
            
            key = self._get_campaign_key(campaign_name, "cps_calls")
            global_key = self._get_global_key("cps_calls")
            
            # Crear timestamps ligeramente diferentes para cada llamada
            # para mantener precisión en el cálculo de CPS
            for i in range(count):
                timestamp = current_time + (i * 0.001)  # Microsegundos de diferencia
                pipe.zadd(key, {str(timestamp): timestamp})
                pipe.zadd(global_key, {f"{campaign_name}:{timestamp}": timestamp})
            
            # Limpiar antiguas
            min_time = current_time - 60
            pipe.zremrangebyscore(key, 0, min_time)
            pipe.zremrangebyscore(global_key, 0, min_time)
            
            # Ejecutar todo de una vez
            pipe.execute()
            logger.debug(f"📞 {campaign_name}: Registradas {count} llamadas en batch")
            
        except Exception as e:
            logger.error(f"Error registrando batch de llamadas en Redis: {e}")
    
    def get_cps(self, campaign_name: str, window: int = None) -> float:
        """
        Calcula el CPS instantáneo de una campaña con algoritmo adaptativo
        
        Args:
            campaign_name: Nombre de la campaña
            window: Ventana de tiempo en segundos (default: self.cps_window)
            
        Returns:
            CPS (llamadas por segundo) instantáneo
        """
        try:
            key = self._get_campaign_key(campaign_name, "cps_calls")
            current_time = time.time()
            window = window or self.cps_window
            min_time = current_time - window
            
            # Contar llamadas en la ventana de tiempo
            call_count = self.redis_client.zcount(key, min_time, current_time)
            
            if call_count == 0:
                # Intentar con ventana más amplia (10 segundos) para detectar actividad baja
                extended_min_time = current_time - 10
                extended_count = self.redis_client.zcount(key, extended_min_time, current_time)
                if extended_count > 0:
                    # Hay actividad reciente, calcular CPS con ventana extendida
                    return round(extended_count / 10.0, 2)
                return 0.0
            
            # Calcular CPS promedio sobre la ventana
            cps = call_count / window
            
            return round(cps, 2)
            
        except Exception as e:
            logger.error(f"Error calculando CPS: {e}")
            return 0.0
    
    def get_instantaneous_cps(self, campaign_name: str) -> float:
        """
        Calcula el CPS del último segundo (ultra instantáneo)
        
        Args:
            campaign_name: Nombre de la campaña
            
        Returns:
            CPS del último segundo
        """
        return self.get_cps(campaign_name, window=1)
    
    def get_cps_average(self, campaign_name: str) -> float:
        """
        Calcula el CPS promedio de los últimos 60 segundos
        
        Args:
            campaign_name: Nombre de la campaña
            
        Returns:
            CPS promedio
        """
        return self.get_cps(campaign_name, window=60)
    
    def get_global_cps(self, window: int = None) -> float:
        """
        Calcula el CPS global instantáneo de todas las campañas con algoritmo adaptativo
        
        Args:
            window: Ventana de tiempo en segundos (default: self.cps_window)
            
        Returns:
            CPS global instantáneo
        """
        try:
            key = self._get_global_key("cps_calls")
            current_time = time.time()
            window = window or self.cps_window
            min_time = current_time - window
            
            # Contar llamadas en la ventana de tiempo
            call_count = self.redis_client.zcount(key, min_time, current_time)
            
            if call_count == 0:
                # Intentar con ventana más amplia (10 segundos) para detectar actividad baja
                extended_min_time = current_time - 10
                extended_count = self.redis_client.zcount(key, extended_min_time, current_time)
                if extended_count > 0:
                    # Hay actividad reciente, calcular CPS con ventana extendida
                    return round(extended_count / 10.0, 2)
                return 0.0
            
            # Calcular CPS promedio sobre la ventana
            cps = call_count / window
            
            return round(cps, 2)
            
        except Exception as e:
            logger.error(f"Error calculando CPS global: {e}")
            return 0.0
    
    def update_max_cps(self, campaign_name: str, current_cps: float) -> float:
        """
        Actualiza el CPS máximo si el CPS actual es mayor
        
        Args:
            campaign_name: Nombre de la campaña
            current_cps: CPS actual
            
        Returns:
            CPS máximo actualizado
        """
        try:
            key = self._get_campaign_key(campaign_name, "cps_max")
            
            # Obtener CPS máximo actual
            max_cps_str = self.redis_client.get(key)
            max_cps = float(max_cps_str) if max_cps_str else 0.0
            
            # Actualizar si el actual es mayor
            if current_cps > max_cps:
                self.redis_client.set(key, str(current_cps))
                logger.debug(f"📈 Nuevo CPS máximo para {campaign_name}: {current_cps:.2f}")
                return current_cps
            
            return max_cps
            
        except Exception as e:
            logger.error(f"Error actualizando CPS máximo: {e}")
            return 0.0
    
    def get_max_cps(self, campaign_name: str) -> float:
        """
        Obtiene el CPS máximo alcanzado por una campaña
        
        Args:
            campaign_name: Nombre de la campaña
            
        Returns:
            CPS máximo
        """
        try:
            key = self._get_campaign_key(campaign_name, "cps_max")
            max_cps_str = self.redis_client.get(key)
            return float(max_cps_str) if max_cps_str else 0.0
        except Exception as e:
            logger.error(f"Error obteniendo CPS máximo: {e}")
            return 0.0
    
    def reset_max_cps(self, campaign_name: str):
        """
        Reinicia el CPS máximo de una campaña
        
        Args:
            campaign_name: Nombre de la campaña
        """
        try:
            key = self._get_campaign_key(campaign_name, "cps_max")
            self.redis_client.delete(key)
            logger.info(f"🔄 CPS máximo reiniciado para {campaign_name}")
        except Exception as e:
            logger.error(f"Error reiniciando CPS máximo: {e}")
    
    def get_cps_diagnostics(self, campaign_name: str) -> Dict:
        """
        Obtiene información de diagnóstico del cálculo de CPS
        
        Args:
            campaign_name: Nombre de la campaña
            
        Returns:
            Diccionario con información de diagnóstico
        """
        try:
            key = self._get_campaign_key(campaign_name, "cps_calls")
            current_time = time.time()
            
            # Contar llamadas en diferentes ventanas
            calls_1s = self.redis_client.zcount(key, current_time - 1, current_time)
            calls_3s = self.redis_client.zcount(key, current_time - 3, current_time)
            calls_5s = self.redis_client.zcount(key, current_time - 5, current_time)
            calls_10s = self.redis_client.zcount(key, current_time - 10, current_time)
            calls_60s = self.redis_client.zcount(key, current_time - 60, current_time)
            total_calls = self.redis_client.zcard(key)
            
            return {
                "campaign_name": campaign_name,
                "total_in_buffer": total_calls,
                "calls_last_1s": calls_1s,
                "calls_last_3s": calls_3s,
                "calls_last_5s": calls_5s,
                "calls_last_10s": calls_10s,
                "calls_last_60s": calls_60s,
                "cps_1s": round(calls_1s / 1, 2) if calls_1s > 0 else 0.0,
                "cps_3s": round(calls_3s / 3, 2) if calls_3s > 0 else 0.0,
                "cps_5s": round(calls_5s / 5, 2) if calls_5s > 0 else 0.0,
                "cps_10s": round(calls_10s / 10, 2) if calls_10s > 0 else 0.0,
                "cps_60s": round(calls_60s / 60, 2) if calls_60s > 0 else 0.0,
                "current_cps": self.get_cps(campaign_name),
                "max_cps": self.get_max_cps(campaign_name),
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error obteniendo diagnósticos de CPS: {e}")
            return {"error": str(e)}
    
    def set_campaign_stats(self, campaign_name: str, stats: Dict):
        """
        Almacena estadísticas detalladas de una campaña en Redis
        
        Args:
            campaign_name: Nombre de la campaña
            stats: Diccionario con todas las estadísticas
        """
        try:
            key = self._get_campaign_key(campaign_name, "stats")
            stats['timestamp'] = datetime.now().isoformat()
            stats['campaign_name'] = campaign_name
            
            # Calcular CPS actual con ventana adaptativa para mejor precisión
            current_cps = self.get_cps(campaign_name)
            stats['cps'] = current_cps
            
            # Actualizar y almacenar CPS máximo
            max_cps = self.update_max_cps(campaign_name, current_cps)
            stats['cps_max'] = max_cps
            
            # Guardar como hash en Redis
            self.redis_client.hset(
                key,
                mapping={k: json.dumps(v) for k, v in stats.items()}
            )
            
            # Establecer TTL de 24 horas
            self.redis_client.expire(key, 86400)
            
            if current_cps > 0:
                logger.debug(f"📊 Estadísticas almacenadas en Redis para {campaign_name} (CPS: {current_cps:.2f}, Max: {max_cps:.2f})")
            
        except Exception as e:
            logger.error(f"Error almacenando estadísticas en Redis: {e}")

    def get_campaign_stats(self, campaign_name: str) -> Dict:
        """
        Obtiene estadísticas completas de una campaña desde Redis
        
        Args:
            campaign_name: Nombre de la campaña
            
        Returns:
            Diccionario con estadísticas
        """
        try:
            # Primero intentar obtener stats almacenadas directamente
            stats_key = self._get_campaign_key(campaign_name, "stats")
            stored_stats = self.redis_client.hgetall(stats_key)
            
            if stored_stats:
                stats = {k: json.loads(v) for k, v in stored_stats.items()}
                # Actualizar CPS en tiempo real con ventana adaptativa
                current_cps = self.get_cps(campaign_name)
                stats['cps'] = current_cps
                # Actualizar CPS máximo
                stats['cps_max'] = self.update_max_cps(campaign_name, current_cps)
                stats['timestamp'] = datetime.now().isoformat()
                return stats
            
            # Si no hay stats almacenadas, calcular desde llamadas
            calls = self.get_all_calls(campaign_name)
            
            # Calcular CPS actual con precisión
            current_cps = self.get_cps(campaign_name)
            
            stats = {
                "campaign_name": campaign_name,
                "total_calls": len(calls),
                "calls_by_state": {},
                "active_calls": 0,
                "cps": current_cps,
                "cps_max": self.update_max_cps(campaign_name, current_cps),
                "timestamp": datetime.now().isoformat()
            }
            
            # Contar llamadas por estado
            for call in calls:
                estado = call.get("estado", "unknown")
                stats["calls_by_state"][estado] = stats["calls_by_state"].get(estado, 0) + 1
                
                # Contar llamadas activas (no terminales)
                if not self._is_terminal_state(estado):
                    stats["active_calls"] += 1
            
            return stats
            
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas de campaña: {e}")
            return {
                "campaign_name": campaign_name,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def get_all_campaigns_stats(self) -> List[Dict]:
        """
        Obtiene estadísticas de todas las campañas
        
        Returns:
            Lista de estadísticas por campaña
        """
        try:
            # Obtener todas las claves de campañas
            pattern = "campaign:*:calls"
            campaign_keys = self.redis_client.keys(pattern)
            
            stats_list = []
            for key in campaign_keys:
                # Extraer nombre de campaña de la clave
                campaign_name = key.split(':')[1]
                stats = self.get_campaign_stats(campaign_name)
                stats_list.append(stats)
            
            return stats_list
            
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas globales: {e}")
            return []
    
    def clear_campaign_data(self, campaign_name: str):
        """
        Limpia todos los datos de una campaña en Redis, incluyendo llamadas activas globales
        
        Args:
            campaign_name: Nombre de la campaña
        """
        try:
            # 1. Eliminar todas las claves específicas de la campaña
            pattern = f"campaign:{campaign_name}:*"
            keys = self.redis_client.keys(pattern)
            
            if keys:
                self.redis_client.delete(*keys)
                logger.info(f"✅ {len(keys)} claves de campaña {campaign_name} eliminadas de Redis")
            
            # 2. Limpiar llamadas de esta campaña del registro global
            try:
                global_key = self._get_global_key("active_calls")
                all_calls = self.redis_client.hgetall(global_key)
                
                deleted_count = 0
                for uuid, call_json in all_calls.items():
                    try:
                        call_data = json.loads(call_json)
                        if call_data.get("campaign_name") == campaign_name:
                            self.redis_client.hdel(global_key, uuid)
                            deleted_count += 1
                    except Exception:
                        continue
                
                if deleted_count > 0:
                    logger.info(f"✅ {deleted_count} llamadas globales de {campaign_name} eliminadas de Redis")
            except Exception as e:
                logger.warning(f"⚠️ Error limpiando llamadas globales: {e}")
            
            # 3. Limpiar registros de CPS global de esta campaña
            try:
                global_cps_key = self._get_global_key("cps_calls")
                all_cps = self.redis_client.zrange(global_cps_key, 0, -1)
                
                deleted_cps = 0
                for entry in all_cps:
                    if entry.startswith(f"{campaign_name}:"):
                        self.redis_client.zrem(global_cps_key, entry)
                        deleted_cps += 1
                
                if deleted_cps > 0:
                    logger.info(f"✅ {deleted_cps} entradas CPS de {campaign_name} eliminadas de Redis")
            except Exception as e:
                logger.warning(f"⚠️ Error limpiando CPS global: {e}")
            
            logger.info(f"✅ Datos de campaña {campaign_name} completamente eliminados de Redis")
                
        except Exception as e:
            logger.error(f"❌ Error limpiando datos de campaña en Redis: {e}")
    
    def ping(self) -> bool:
        """
        Verifica la conexión con Redis
        
        Returns:
            True si la conexión es exitosa
        """
        try:
            return self.redis_client.ping()
        except Exception as e:
            logger.error(f"Error conectando con Redis: {e}")
            return False
    
    def create_stats_table(self, campaign_name: str) -> bool:
        """
        Crea una tabla de estadísticas para una campaña con nomenclatura estadisticas_{campaign_name}
        
        Args:
            campaign_name: Nombre de la campaña
            
        Returns:
            True si la tabla se creó exitosamente o ya existe
        """
        try:
            table_name = f"estadisticas_{campaign_name}"
            
            # Crear tabla con todas las columnas de estadísticas
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                
                -- Estadísticas generales
                total_calls INT DEFAULT 0,
                calls_sent INT DEFAULT 0,
                calls_ringing INT DEFAULT 0,
                calls_answered INT DEFAULT 0,
                calls_completed INT DEFAULT 0,
                calls_failed INT DEFAULT 0,
                calls_busy INT DEFAULT 0,
                calls_no_answer INT DEFAULT 0,
                calls_pending INT DEFAULT 0,
                
                -- Estadísticas AMD
                amd_human INT DEFAULT 0,
                amd_machine INT DEFAULT 0,
                amd_notsure INT DEFAULT 0,
                
                -- Estadísticas de errores específicos
                calls_no_route INT DEFAULT 0,
                calls_invalid_number INT DEFAULT 0,
                calls_codec_error INT DEFAULT 0,
                calls_timeout INT DEFAULT 0,
                
                -- Estadísticas de hangup
                hangup_server INT DEFAULT 0,
                hangup_client INT DEFAULT 0,
                calls_cancelled INT DEFAULT 0,
                
                -- Métricas de rendimiento
                cps FLOAT DEFAULT 0.0,
                cps_max FLOAT DEFAULT 0.0,
                active_calls INT DEFAULT 0,
                
                -- Información adicional
                campaign_name VARCHAR(255) NOT NULL,
                
                INDEX idx_timestamp (timestamp),
                INDEX idx_campaign (campaign_name),
                UNIQUE KEY unique_campaign_hour (campaign_name, timestamp)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """
            
            with self.mysql_engine.begin() as conn:
                conn.execute(text(create_table_sql))
                logger.info(f"✅ Tabla {table_name} creada o ya existe")
                
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creando tabla de estadísticas para {campaign_name}: {e}")
            return False
    
    def save_stats_to_mysql(self, campaign_name: str, stats: Dict) -> bool:
        """
        Guarda/actualiza las estadísticas desde Redis a la tabla MySQL estadisticas_{campaign_name}
        Usa UPSERT (INSERT ... ON DUPLICATE KEY UPDATE) para evitar duplicados
        
        Args:
            campaign_name: Nombre de la campaña
            stats: Diccionario con las estadísticas
            
        Returns:
            True si se guardó exitosamente
        """
        try:
            # Crear tabla si no existe
            self.create_stats_table(campaign_name)
            
            table_name = f"estadisticas_{campaign_name}"
            
            # Preparar datos para actualización
            update_data = {
                'timestamp': datetime.now(),
                'total_calls': stats.get('total_calls', 0),
                'calls_sent': stats.get('calls_sent', 0),
                'calls_ringing': stats.get('calls_ringing', 0),
                'calls_answered': stats.get('calls_answered', 0),
                'calls_completed': stats.get('calls_completed', 0),
                'calls_failed': stats.get('calls_failed', 0),
                'calls_busy': stats.get('calls_busy', 0),
                'calls_no_answer': stats.get('calls_no_answer', 0),
                'calls_pending': stats.get('calls_pending', 0),
                'amd_human': stats.get('amd_human', 0),
                'amd_machine': stats.get('amd_machine', 0),
                'amd_notsure': stats.get('amd_notsure', 0),
                'calls_no_route': stats.get('calls_no_route', 0),
                'calls_invalid_number': stats.get('calls_invalid_number', 0),
                'calls_codec_error': stats.get('calls_codec_error', 0),
                'calls_timeout': stats.get('calls_timeout', 0),
                'hangup_server': stats.get('hangup_server', 0),
                'hangup_client': stats.get('hangup_client', 0),
                'calls_cancelled': stats.get('calls_cancelled', 0),
                'cps': stats.get('cps', 0.0),
                'cps_max': stats.get('cps_max', 0.0),
                'active_calls': stats.get('active_calls', 0),
                'campaign_name': campaign_name
            }
            
            with self.mysql_engine.begin() as conn:
                # Verificar si existe el registro
                check_sql = f"SELECT COUNT(*) FROM `{table_name}` WHERE campaign_name = :campaign_name"
                exists = conn.execute(text(check_sql), {"campaign_name": campaign_name}).scalar()
                
                if exists:
                    # Solo UPDATE si existe
                    update_fields = ', '.join([f"`{k}` = :{k}" for k in update_data.keys() if k != 'campaign_name'])
                    update_sql = f"""
                        UPDATE `{table_name}`
                        SET {update_fields}
                        WHERE campaign_name = :campaign_name
                    """
                    conn.execute(text(update_sql), update_data)
                    logger.debug(f"📊 Estadísticas actualizadas en MySQL: {table_name}")
                else:
                    # INSERT solo si no existe
                    columns = ', '.join([f"`{k}`" for k in update_data.keys()])
                    placeholders = ', '.join([f":{k}" for k in update_data.keys()])
                    insert_sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
                    conn.execute(text(insert_sql), update_data)
                    logger.debug(f"📊 Primera inserción en MySQL: {table_name}")
                
            return True
            
        except Exception as e:
            logger.error(f"❌ Error guardando estadísticas en MySQL para {campaign_name}: {e}")
            return False
    
    def get_stats_history(self, campaign_name: str, limit: int = 100) -> List[Dict]:
        """
        Obtiene el historial de estadísticas desde MySQL
        
        Args:
            campaign_name: Nombre de la campaña
            limit: Número máximo de registros a retornar
            
        Returns:
            Lista de diccionarios con las estadísticas históricas
        """
        try:
            table_name = f"estadisticas_{campaign_name}"
            
            # Verificar si la tabla existe
            with self.mysql_engine.connect() as conn:
                table_exists = conn.execute(text("""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = DATABASE() 
                    AND table_name = :table_name
                """), {"table_name": table_name}).scalar()
                
                if not table_exists:
                    logger.warning(f"Tabla {table_name} no existe")
                    return []
                
                # Obtener registros ordenados por timestamp descendente
                result = conn.execute(text(f"""
                    SELECT * FROM `{table_name}`
                    ORDER BY timestamp DESC
                    LIMIT :limit
                """), {"limit": limit})
                
                stats_history = []
                for row in result:
                    row_dict = dict(row._mapping)
                    # Convertir datetime a string
                    if 'timestamp' in row_dict and row_dict['timestamp']:
                        row_dict['timestamp'] = row_dict['timestamp'].isoformat()
                    stats_history.append(row_dict)
                
                return stats_history
                
        except Exception as e:
            logger.error(f"❌ Error obteniendo historial de estadísticas para {campaign_name}: {e}")
            return []
    
    def get_latest_stats_from_mysql(self, campaign_name: str) -> Optional[Dict]:
        """
        Obtiene las estadísticas más recientes desde MySQL
        
        Args:
            campaign_name: Nombre de la campaña
            
        Returns:
            Diccionario con las estadísticas más recientes o None
        """
        try:
            history = self.get_stats_history(campaign_name, limit=1)
            return history[0] if history else None
        except Exception as e:
            logger.error(f"❌ Error obteniendo última estadística para {campaign_name}: {e}")
            return None
    
    def sync_redis_to_mysql(self, campaign_name: str) -> bool:
        """
        Sincroniza las estadísticas de Redis a MySQL
        
        Args:
            campaign_name: Nombre de la campaña
            
        Returns:
            True si la sincronización fue exitosa
        """
        try:
            # Obtener stats desde Redis
            stats = self.get_campaign_stats(campaign_name)
            
            if not stats or 'error' in stats:
                logger.warning(f"No hay estadísticas en Redis para {campaign_name}")
                return False
            
            # Guardar en MySQL
            return self.save_stats_to_mysql(campaign_name, stats)
            
        except Exception as e:
            logger.error(f"❌ Error sincronizando Redis a MySQL para {campaign_name}: {e}")
            return False
    
    def auto_sync_enabled_campaigns(self) -> Dict[str, bool]:
        """
        Sincroniza automáticamente todas las campañas activas desde Redis a MySQL
        
        Returns:
            Diccionario con el resultado de sincronización por campaña
        """
        try:
            # Obtener todas las campañas activas desde Redis
            pattern = "campaign:*:stats"
            campaign_keys = self.redis_client.keys(pattern)
            
            results = {}
            for key in campaign_keys:
                # Extraer nombre de campaña
                campaign_name = key.split(':')[1]
                results[campaign_name] = self.sync_redis_to_mysql(campaign_name)
            
            if results:
                logger.info(f"🔄 Sincronización automática completada: {sum(results.values())}/{len(results)} campañas")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Error en sincronización automática: {e}")
            return {}

