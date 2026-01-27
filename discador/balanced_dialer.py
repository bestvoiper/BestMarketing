"""
Balanceador de Carga para Discador
Distribuye conexiones WebSocket entre los puertos dinámicos del discador.
Se integra con el DynamicPortManager de check_queue.py para conocer los puertos activos.

Puerto de escucha: 8080 (configurable)
Puertos backend: Dinámicos según agentes conectados (8770-8789 por defecto)
"""
import asyncio
import websockets
import json
import random
import logging
import time
import aiohttp
from collections import defaultdict, deque
from typing import Dict, Set, List, Optional, Any
import sys
import signal
import gc

# ============================================================================
# CONFIGURACIÓN
# ============================================================================
LISTEN_PORT = 8082                      # Puerto donde escucha el balanceador
MONITOR_WS_URL = "ws://127.0.0.1:8767"  # WebSocket del monitor de colas (check_queue.py)
BASE_DYNAMIC_PORT = 8770                # Puerto base de los puertos dinámicos
MAX_DYNAMIC_PORT = 8789                 # Puerto máximo de los puertos dinámicos
MAX_CONNECTIONS_PER_PORT = 50           # Máximo conexiones por puerto dinámico
HEALTH_CHECK_INTERVAL = 10              # Intervalo de health check (segundos)
CONNECTION_TIMEOUT = 300                # Timeout de conexiones (segundos)
CONNECTION_CLEANUP_INTERVAL = 30        # Limpieza de conexiones (segundos)
SYNC_WITH_MONITOR_INTERVAL = 5          # Sincronizar con monitor cada X segundos

# ============================================================================
# LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DialerLoadBalancer:
    """
    Balanceador de carga para el discador.
    Se sincroniza con check_queue.py para saber qué puertos están activos.
    """
    
    def __init__(self):
        # Puertos activos (se actualizan dinámicamente desde el monitor)
        self.active_ports: List[int] = []
        self.backend_stats: Dict[int, Dict] = {}
        
        # Conexiones activas
        self.active_connections: Dict[str, Dict] = {}
        self.connection_history: deque = deque(maxlen=1000)
        
        # Estado del sistema
        self.start_time = time.time()
        self.total_connections_served = 0
        self.logged_agents = 0
        self.last_monitor_sync = 0
        self.monitor_connected = False
        
        # WebSocket al monitor
        self.monitor_ws: Optional[websockets.WebSocketClientProtocol] = None
        
        logger.info(f"📡 DialerLoadBalancer inicializado")
        logger.info(f"   Puerto de escucha: {LISTEN_PORT}")
        logger.info(f"   Monitor: {MONITOR_WS_URL}")
        logger.info(f"   Rango de puertos: {BASE_DYNAMIC_PORT}-{MAX_DYNAMIC_PORT}")
    
    def _init_port_stats(self, port: int):
        """Inicializar estadísticas para un puerto"""
        if port not in self.backend_stats:
            self.backend_stats[port] = {
                'connections': 0,
                'healthy': True,
                'last_check': 0,
                'total_served': 0,
                'avg_duration': 0,
                'last_assigned': 0,
                'errors': 0,
                'response_time': 0
            }
    
    async def connect_to_monitor(self):
        """Conectar al WebSocket del monitor de colas"""
        while True:
            try:
                if self.monitor_ws is None or self.monitor_ws.closed:
                    logger.info(f"🔌 Conectando al monitor en {MONITOR_WS_URL}...")
                    self.monitor_ws = await websockets.connect(
                        MONITOR_WS_URL,
                        ping_interval=30,
                        ping_timeout=10
                    )
                    self.monitor_connected = True
                    logger.info("✅ Conectado al monitor de colas")
                    
                    # Solicitar información de puertos dinámicos
                    await self.monitor_ws.send(json.dumps({
                        "type": "get_dynamic_ports"
                    }))
                
                # Escuchar mensajes del monitor
                async for message in self.monitor_ws:
                    try:
                        data = json.loads(message)
                        await self._process_monitor_message(data)
                    except json.JSONDecodeError:
                        logger.warning("Mensaje inválido del monitor")
                        
            except websockets.exceptions.ConnectionClosed:
                logger.warning("⚠️ Conexión con monitor cerrada, reconectando...")
                self.monitor_connected = False
                self.monitor_ws = None
            except Exception as e:
                logger.error(f"❌ Error conectando al monitor: {e}")
                self.monitor_connected = False
                self.monitor_ws = None
            
            await asyncio.sleep(5)
    
    async def _process_monitor_message(self, data: Dict):
        """Procesar mensaje recibido del monitor"""
        msg_type = data.get("type", "")
        
        if msg_type == "dynamic_ports":
            # Actualizar lista de puertos activos
            ports_data = data.get("data", {})
            self._update_active_ports(ports_data)
        
        elif msg_type == "queue_update":
            # Actualización de colas - extraer info de puertos dinámicos
            queue_data = data.get("data", {})
            dynamic_ports = queue_data.get("dynamic_ports")
            if dynamic_ports:
                self._update_active_ports(dynamic_ports)
            
            # Actualizar número de agentes
            self.logged_agents = queue_data.get("total_logged_agents", 0)
        
        elif msg_type == "initial_data":
            # Datos iniciales
            init_data = data.get("data", {})
            dynamic_ports = init_data.get("dynamic_ports")
            if dynamic_ports:
                self._update_active_ports(dynamic_ports)
    
    def _update_active_ports(self, ports_data: Dict):
        """Actualizar lista de puertos activos desde datos del monitor"""
        if not ports_data.get("enabled", False):
            # Puertos dinámicos deshabilitados - usar puerto principal
            self.active_ports = [8767]  # Puerto principal del monitor
            logger.info("⚠️ Puertos dinámicos deshabilitados, usando puerto principal 8767")
            return
        
        new_ports = ports_data.get("active_ports", [])
        
        if new_ports != self.active_ports:
            old_ports = set(self.active_ports)
            new_ports_set = set(new_ports)
            
            added = new_ports_set - old_ports
            removed = old_ports - new_ports_set
            
            if added:
                logger.info(f"🟢 Puertos agregados: {sorted(added)}")
                for port in added:
                    self._init_port_stats(port)
            
            if removed:
                logger.info(f"🔴 Puertos removidos: {sorted(removed)}")
            
            self.active_ports = new_ports
            self.logged_agents = ports_data.get("current_logged_agents", 0)
            
            logger.info(f"📊 Puertos activos: {len(self.active_ports)} "
                       f"(agentes: {self.logged_agents})")
        
        self.last_monitor_sync = time.time()
    
    async def sync_with_monitor(self):
        """Sincronizar periódicamente con el monitor"""
        while True:
            try:
                if self.monitor_ws and not self.monitor_ws.closed:
                    await self.monitor_ws.send(json.dumps({
                        "type": "get_dynamic_ports"
                    }))
            except Exception as e:
                logger.debug(f"Error sincronizando con monitor: {e}")
            
            await asyncio.sleep(SYNC_WITH_MONITOR_INTERVAL)
    
    async def health_check(self):
        """Verificar salud de los puertos activos"""
        while True:
            try:
                current_time = time.time()
                
                for port in list(self.active_ports):
                    if port not in self.backend_stats:
                        self._init_port_stats(port)
                        continue
                    
                    # Solo verificar si ha pasado suficiente tiempo
                    if current_time - self.backend_stats[port]['last_check'] < HEALTH_CHECK_INTERVAL:
                        continue
                    
                    try:
                        # Test de conexión TCP simple - solo verificar que el puerto está escuchando
                        # No hacemos WebSocket completo porque VOSK requiere parámetros específicos
                        reader, writer = await asyncio.wait_for(
                            asyncio.open_connection('127.0.0.1', port),
                            timeout=3
                        )
                        writer.close()
                        await writer.wait_closed()
                        
                        if not self.backend_stats[port]['healthy']:
                            logger.info(f"✅ Puerto {port} recuperado")
                        self.backend_stats[port]['healthy'] = True
                        
                    except (ConnectionRefusedError, OSError, asyncio.TimeoutError) as e:
                        # Estos son errores reales de conexión
                        if self.backend_stats[port]['healthy']:
                            logger.warning(f"❌ Puerto {port} no responde: {e}")
                        self.backend_stats[port]['healthy'] = False
                    except Exception as e:
                        # Otros errores - loggear pero no marcar como caído si es código 1000
                        error_str = str(e)
                        if "1000" in error_str or "OK" in error_str:
                            # Cierre normal - el puerto está activo
                            if not self.backend_stats[port]['healthy']:
                                logger.info(f"✅ Puerto {port} recuperado")
                            self.backend_stats[port]['healthy'] = True
                        else:
                            if self.backend_stats[port]['healthy']:
                                logger.warning(f"❌ Puerto {port} error: {e}")
                            self.backend_stats[port]['healthy'] = False
                    
                    self.backend_stats[port]['last_check'] = current_time
                
                # Log estadísticas cada 5 minutos
                if int(current_time) % 300 == 0:
                    await self.log_stats()
                    
            except Exception as e:
                logger.error(f"Error en health check: {e}")
            
            await asyncio.sleep(HEALTH_CHECK_INTERVAL)
    
    async def cleanup_stale_connections(self):
        """Limpiar conexiones obsoletas"""
        while True:
            try:
                current_time = time.time()
                stale_connections = []
                
                for conn_id, conn_info in list(self.active_connections.items()):
                    # Verificar si la conexión está cerrada
                    if conn_info['client_ws'].closed:
                        stale_connections.append(conn_id)
                        continue
                    
                    if conn_info.get('backend_ws') and conn_info['backend_ws'].closed:
                        stale_connections.append(conn_id)
                        continue
                    
                    # Verificar timeout
                    if current_time - conn_info['start_time'] > CONNECTION_TIMEOUT:
                        stale_connections.append(conn_id)
                        continue
                
                # Limpiar conexiones obsoletas
                for conn_id in stale_connections:
                    conn_info = self.active_connections.pop(conn_id, None)
                    if conn_info:
                        await self._cleanup_connection(conn_info)
                
                if stale_connections:
                    logger.info(f"🧹 Limpiadas {len(stale_connections)} conexiones obsoletas")
                
                # Verificar contadores
                await self.verify_connection_counts()
                
                if len(stale_connections) > 10:
                    gc.collect()
                    
            except Exception as e:
                logger.error(f"Error en cleanup: {e}")
            
            await asyncio.sleep(CONNECTION_CLEANUP_INTERVAL)
    
    async def _cleanup_connection(self, conn_info: Dict):
        """Limpiar una conexión específica"""
        try:
            # Decrementar contador del backend
            backend_port = conn_info.get('backend_port')
            if backend_port and backend_port in self.backend_stats:
                self.backend_stats[backend_port]['connections'] = max(
                    0, self.backend_stats[backend_port]['connections'] - 1
                )
            
            # Cerrar WebSockets
            if conn_info.get('backend_ws') and not conn_info['backend_ws'].closed:
                try:
                    await asyncio.wait_for(conn_info['backend_ws'].close(), timeout=2)
                except:
                    pass
            
            if not conn_info['client_ws'].closed:
                try:
                    await asyncio.wait_for(conn_info['client_ws'].close(), timeout=2)
                except:
                    pass
                    
        except Exception as e:
            logger.debug(f"Error en cleanup de conexión: {e}")
    
    async def verify_connection_counts(self):
        """Verificar y corregir inconsistencias en contadores"""
        real_counts = defaultdict(int)
        for conn_info in self.active_connections.values():
            if conn_info.get('backend_port'):
                real_counts[conn_info['backend_port']] += 1
        
        for port in self.active_ports:
            if port not in self.backend_stats:
                continue
            
            real_count = real_counts.get(port, 0)
            stored_count = self.backend_stats[port]['connections']
            
            if real_count != stored_count:
                logger.warning(f"⚠️ Corrigiendo contador puerto {port}: {stored_count} -> {real_count}")
                self.backend_stats[port]['connections'] = real_count
    
    async def log_stats(self):
        """Mostrar estadísticas del balanceador"""
        await self.verify_connection_counts()
        
        total_connections = sum(
            self.backend_stats.get(p, {}).get('connections', 0) 
            for p in self.active_ports
        )
        healthy_ports = sum(
            1 for p in self.active_ports 
            if self.backend_stats.get(p, {}).get('healthy', False)
        )
        uptime = time.time() - self.start_time
        
        logger.info("=" * 70)
        logger.info(f"📊 ESTADÍSTICAS DEL BALANCEADOR DE DISCADOR")
        logger.info(f"   Puerto de escucha: {LISTEN_PORT}")
        logger.info(f"   Conexiones activas: {len(self.active_connections)}")
        logger.info(f"   Total servidas: {self.total_connections_served}")
        logger.info(f"   Uptime: {uptime/3600:.1f}h")
        logger.info(f"   Agentes conectados: {self.logged_agents}")
        logger.info(f"   Puertos activos: {len(self.active_ports)} ({healthy_ports} saludables)")
        logger.info(f"   Monitor conectado: {'✅' if self.monitor_connected else '❌'}")
        
        if self.active_ports:
            logger.info(f"   Puertos: {sorted(self.active_ports)}")
            
            # Detalles por puerto
            for port in sorted(self.active_ports):
                if port in self.backend_stats:
                    stats = self.backend_stats[port]
                    status = "✅" if stats['healthy'] else "❌"
                    logger.info(
                        f"      {port}: {stats['connections']} conn, "
                        f"{stats['total_served']} total, "
                        f"{stats['errors']} err {status}"
                    )
        
        logger.info("=" * 70)
    
    def get_best_backend(self) -> Optional[int]:
        """Seleccionar el mejor puerto para una nueva conexión"""
        if not self.active_ports:
            logger.error("❌ No hay puertos activos disponibles")
            return None
        
        current_time = time.time()
        
        # Filtrar puertos saludables con capacidad
        available_ports = []
        for port in self.active_ports:
            if port not in self.backend_stats:
                self._init_port_stats(port)
            
            stats = self.backend_stats[port]
            if stats['healthy'] and stats['connections'] < MAX_CONNECTIONS_PER_PORT:
                available_ports.append(port)
        
        if not available_ports:
            # Si no hay saludables, usar cualquiera con capacidad
            available_ports = [
                p for p in self.active_ports
                if self.backend_stats.get(p, {}).get('connections', 0) < MAX_CONNECTIONS_PER_PORT
            ]
        
        if not available_ports:
            logger.error("❌ Todos los puertos están llenos")
            # Usar el que tenga menos conexiones
            return min(self.active_ports, 
                      key=lambda p: self.backend_stats.get(p, {}).get('connections', 999))
        
        # Algoritmo: Least Connections con penalty por errores
        def score(port):
            stats = self.backend_stats.get(port, {})
            conn_score = stats.get('connections', 0) * 10
            error_score = stats.get('errors', 0) * 5
            # Bonus para puertos no usados recientemente
            time_bonus = 0
            if current_time - stats.get('last_assigned', 0) > 5:
                time_bonus = -5
            return conn_score + error_score + time_bonus
        
        # Elegir el puerto con menor score
        selected = min(available_ports, key=score)
        self.backend_stats[selected]['last_assigned'] = current_time
        
        return selected
    
    async def proxy_websocket(self, client_ws, path):
        """Manejar proxy de WebSocket"""
        backend_port = None
        backend_ws = None
        client_addr = f"{client_ws.remote_address[0]}:{client_ws.remote_address[1]}"
        conn_id = f"{client_addr}_{int(time.time()*1000)}"
        connection_start = time.time()
        connection_success = False
        
        try:
            # Verificar que haya puertos disponibles
            if not self.active_ports:
                logger.warning(f"⚠️ No hay puertos activos para {client_addr}")
                await client_ws.send(json.dumps({
                    "type": "error",
                    "message": "No hay puertos disponibles. Verificar agentes conectados."
                }))
                return
            
            # Seleccionar backend
            backend_port = self.get_best_backend()
            if backend_port is None:
                logger.error(f"❌ No se pudo seleccionar backend para {client_addr}")
                return
            
            backend_url = f"ws://127.0.0.1:{backend_port}{path}"
            
            logger.debug(f"🔗 {client_addr} -> puerto {backend_port}")
            
            # Conectar al backend
            connect_start = time.time()
            backend_ws = await asyncio.wait_for(
                websockets.connect(
                    backend_url,
                    max_size=None,
                    ping_interval=20,      # Coincidir con VOSK
                    ping_timeout=10,       # Coincidir con VOSK
                    close_timeout=5
                ),
                timeout=5
            )
            connect_time = (time.time() - connect_start) * 1000
            
            # Actualizar estadísticas
            stats = self.backend_stats[backend_port]
            if stats['response_time'] == 0:
                stats['response_time'] = connect_time
            else:
                stats['response_time'] = stats['response_time'] * 0.7 + connect_time * 0.3
            
            # Registrar conexión
            conn_info = {
                'client_ws': client_ws,
                'backend_ws': backend_ws,
                'backend_port': backend_port,
                'start_time': connection_start,
                'client_addr': client_addr
            }
            
            self.active_connections[conn_id] = conn_info
            self.backend_stats[backend_port]['connections'] += 1
            self.backend_stats[backend_port]['total_served'] += 1
            self.total_connections_served += 1
            connection_success = True
            
            logger.info(
                f"✅ {client_addr} -> puerto {backend_port} "
                f"(conn: {self.backend_stats[backend_port]['connections']}, "
                f"tiempo: {connect_time:.1f}ms, path: {path})"
            )
            
            # Proxy bidireccional
            try:
                results = await asyncio.gather(
                    self.forward_messages(client_ws, backend_ws, f"FS->VOSK:{backend_port}", conn_id),
                    self.forward_messages(backend_ws, client_ws, f"VOSK:{backend_port}->FS", conn_id),
                    return_exceptions=True
                )
                # Log resultados del gather
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.debug(f"Proxy task {i} terminó con excepción: {result}")
            except Exception as e:
                logger.debug(f"Proxy terminado para {client_addr}: {e}")
            
            # Determinar quién cerró primero
            fs_closed = client_ws.closed
            vosk_closed = backend_ws.closed if backend_ws else True
            close_reason = []
            if fs_closed:
                close_reason.append(f"FS cerrado (code={getattr(client_ws, 'close_code', '?')})")
            if vosk_closed:
                close_reason.append(f"VOSK cerrado (code={getattr(backend_ws, 'close_code', '?') if backend_ws else 'N/A'})")
            logger.info(f"📋 Razón cierre {client_addr}: {', '.join(close_reason)}")
            
        except asyncio.TimeoutError:
            logger.warning(f"❌ Timeout conectando a backend {backend_port}")
            if backend_port and backend_port in self.backend_stats:
                self.backend_stats[backend_port]['errors'] += 1
        except Exception as e:
            logger.error(f"❌ Error en proxy para {client_addr}: {e}")
            if backend_port and backend_port in self.backend_stats:
                self.backend_stats[backend_port]['errors'] += 1
        finally:
            # Limpiar conexión
            duration = time.time() - connection_start
            conn_info = self.active_connections.pop(conn_id, None)
            
            if conn_info:
                await self._cleanup_connection(conn_info)
                
                if connection_success and backend_port:
                    stats = self.backend_stats[backend_port]
                    if stats['avg_duration'] == 0:
                        stats['avg_duration'] = duration
                    else:
                        stats['avg_duration'] = stats['avg_duration'] * 0.9 + duration * 0.1
            
            elif backend_port and backend_port in self.backend_stats:
                self.backend_stats[backend_port]['connections'] = max(
                    0, self.backend_stats[backend_port]['connections'] - 1
                )
            
            if backend_port:
                logger.info(
                    f"🔌 Cerrada {client_addr} "
                    f"(puerto {backend_port}: {self.backend_stats.get(backend_port, {}).get('connections', 0)} conn, "
                    f"duración: {duration:.1f}s)"
                )
    
    async def forward_messages(self, source, destination, direction, conn_id):
        """Reenviar mensajes entre WebSockets (texto y binario)"""
        message_count = 0
        bytes_transferred = 0
        first_message_time = None
        last_message_time = None
        try:
            async for message in source:
                if first_message_time is None:
                    first_message_time = time.time()
                    logger.info(f"[{direction}] Primer mensaje recibido")
                last_message_time = time.time()
                
                if conn_id not in self.active_connections:
                    logger.info(f"[{direction}] Conexión no encontrada, terminando (msgs: {message_count})")
                    break
                if destination.closed:
                    logger.info(f"[{direction}] Destino cerrado, terminando (msgs: {message_count})")
                    break
                    
                # Reenviar mensaje (texto o binario)
                await destination.send(message)
                message_count += 1
                if isinstance(message, bytes):
                    bytes_transferred += len(message)
                
                # Log cada 100 mensajes binarios (audio)
                if message_count % 100 == 0 and isinstance(message, bytes):
                    logger.info(f"[{direction}] {message_count} msgs, {bytes_transferred/1024:.1f}KB transferidos")
                
        except websockets.exceptions.ConnectionClosedOK:
            # Cierre normal (código 1000)
            logger.info(f"[{direction}] Cierre normal code=1000 (msgs: {message_count}, bytes: {bytes_transferred})")
        except websockets.exceptions.ConnectionClosedError as e:
            # Cierre con error
            logger.info(f"[{direction}] Cierre con error: code={e.code} reason='{e.reason}' (msgs: {message_count})")
        except Exception as e:
            logger.warning(f"[{direction}] Error forwarding: {type(e).__name__}: {e} (msgs: {message_count})")
        finally:
            duration = (last_message_time - first_message_time) if first_message_time and last_message_time else 0
            logger.info(f"[{direction}] FIN - {message_count} msgs, {bytes_transferred/1024:.1f}KB, duración audio: {duration:.1f}s")


# Instancia global
balancer = DialerLoadBalancer()


async def handle_websocket(websocket, path):
    """Handler principal para conexiones WebSocket"""
    await balancer.proxy_websocket(websocket, path)


async def main():
    """Función principal del balanceador"""
    logger.info("=" * 70)
    logger.info("🚀 BALANCEADOR DE DISCADOR")
    logger.info("=" * 70)
    logger.info(f"   Puerto de escucha: {LISTEN_PORT}")
    logger.info(f"   Monitor WebSocket: {MONITOR_WS_URL}")
    logger.info(f"   Rango de puertos: {BASE_DYNAMIC_PORT}-{MAX_DYNAMIC_PORT}")
    logger.info("=" * 70)
    
    # Iniciar tareas de mantenimiento
    tasks = [
        asyncio.create_task(balancer.connect_to_monitor()),
        asyncio.create_task(balancer.sync_with_monitor()),
        asyncio.create_task(balancer.health_check()),
        asyncio.create_task(balancer.cleanup_stale_connections()),
    ]
    
    # Esperar un momento para conectar al monitor
    await asyncio.sleep(2)
    
    # Iniciar servidor WebSocket
    async with websockets.serve(
        handle_websocket,
        "0.0.0.0",
        LISTEN_PORT,
        ping_interval=None,
        ping_timeout=None,
        close_timeout=5,
        max_size=None,
        compression=None,
        max_queue=100
    ):
        logger.info(f"✅ Balanceador escuchando en ws://0.0.0.0:{LISTEN_PORT}")
        
        try:
            await asyncio.Future()
        except KeyboardInterrupt:
            logger.info("🛑 Deteniendo balanceador...")
        finally:
            # Cancelar tareas
            for task in tasks:
                task.cancel()
            
            # Limpiar conexiones
            logger.info("🧹 Limpiando conexiones...")
            for conn_info in list(balancer.active_connections.values()):
                await balancer._cleanup_connection(conn_info)
            balancer.active_connections.clear()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("💀 Balanceador detenido")
    except Exception as e:
        logger.error(f"💥 Error crítico: {e}")
        sys.exit(1)
