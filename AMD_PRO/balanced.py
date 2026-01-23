import asyncio
import websockets
import json
import random
import logging
import time
import weakref
from collections import defaultdict, deque
import sys
import signal
import gc

# Configuración
LISTEN_PORT = 8082
VOSK_PORTS = list(range(8101, 8201))  # 100 puertos
MAX_CONNECTIONS_PER_PORT = 100
HEALTH_CHECK_INTERVAL = 30
CONNECTION_TIMEOUT = 300
CONNECTION_CLEANUP_INTERVAL = 60  # Limpiar conexiones cada minuto

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LoadBalancer:
    def __init__(self):
        self.backend_stats = {port: {
            'connections': 0,           # Conexiones actuales
            'healthy': True,            # Estado de salud
            'last_check': 0,            # Última verificación de salud
            'total_served': 0,          # Total de conexiones servidas (histórico)
            'avg_duration': 0,          # Duración promedio de conexiones (segundos)
            'last_assigned': 0,         # Timestamp de última asignación
            'errors': 0,                # Contador de errores
            'response_time': 0          # Tiempo de respuesta promedio (ms)
        } for port in VOSK_PORTS}
        self.active_connections = {}  # Cambiar a dict para mejor tracking
        self.connection_history = deque(maxlen=1000)
        self.start_time = time.time()
        self.total_connections_served = 0

    async def health_check(self):
        """Verificar salud de backends periódicamente"""
        while True:
            try:
                current_time = time.time()
                for port in VOSK_PORTS:
                    # Solo verificar si ha pasado suficiente tiempo
                    if current_time - self.backend_stats[port]['last_check'] < HEALTH_CHECK_INTERVAL:
                        continue

                    try:
                        # Test rápido de conexión
                        reader, writer = await asyncio.wait_for(
                            asyncio.open_connection('127.0.0.1', port),
                            timeout=2
                        )
                        writer.close()
                        await writer.wait_closed()

                        if not self.backend_stats[port]['healthy']:
                            logger.info(f"✅ Puerto {port} recuperado")
                        self.backend_stats[port]['healthy'] = True

                    except Exception as e:
                        if self.backend_stats[port]['healthy']:
                            logger.warning(f"❌ Puerto {port} no responde: {e}")
                        self.backend_stats[port]['healthy'] = False

                    self.backend_stats[port]['last_check'] = current_time

                # Log estadísticas cada 5 minutos
                if int(current_time) % 300 == 0:
                    await self.log_stats()

            except Exception as e:
                logger.error(f"Error en health check: {e}")

            await asyncio.sleep(10)

    async def cleanup_stale_connections(self):
        """Limpiar conexiones colgadas periódicamente"""
        while True:
            try:
                current_time = time.time()
                stale_connections = []

                for conn_id, conn_info in list(self.active_connections.items()):
                    # Verificar si la conexión está cerrada
                    if conn_info['client_ws'].closed or (conn_info.get('backend_ws') and conn_info['backend_ws'].closed):
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
                        logger.debug(f"🧹 Conexión obsoleta limpiada: {conn_id}")

                if stale_connections:
                    logger.info(f"🧹 Limpiadas {len(stale_connections)} conexiones obsoletas")

                # CRÍTICO: Verificar contadores cada ciclo de limpieza
                await self.verify_connection_counts()

                # Forzar garbage collection
                if len(stale_connections) > 10:
                    gc.collect()

            except Exception as e:
                logger.error(f"Error en cleanup de conexiones: {e}")

            await asyncio.sleep(CONNECTION_CLEANUP_INTERVAL)

    async def _cleanup_connection(self, conn_info):
        """Limpiar una conexión específica"""
        try:
            # CRÍTICO: Decrementar contador del backend PRIMERO
            if conn_info.get('backend_port') and conn_info['backend_port'] in self.backend_stats:
                old_count = self.backend_stats[conn_info['backend_port']]['connections']
                self.backend_stats[conn_info['backend_port']]['connections'] = max(
                    0, self.backend_stats[conn_info['backend_port']]['connections'] - 1
                )
                new_count = self.backend_stats[conn_info['backend_port']]['connections']
                logger.debug(f"📉 Puerto {conn_info['backend_port']}: {old_count} -> {new_count} conexiones")

            # Cerrar WebSockets con timeout
            if conn_info.get('backend_ws') and not conn_info['backend_ws'].closed:
                try:
                    await asyncio.wait_for(conn_info['backend_ws'].close(), timeout=2)
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout cerrando backend websocket")
                except Exception as e:
                    logger.debug(f"Error cerrando backend websocket: {e}")

            if not conn_info['client_ws'].closed:
                try:
                    await asyncio.wait_for(conn_info['client_ws'].close(), timeout=2)
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout cerrando client websocket")
                except Exception as e:
                    logger.debug(f"Error cerrando client websocket: {e}")

        except Exception as e:
            logger.debug(f"Error en cleanup de conexión: {e}")

    async def log_stats(self):
        """Mostrar estadísticas del balanceador con información detallada"""
        # VERIFICAR Y CORREGIR inconsistencias en contadores
        await self.verify_connection_counts()
        
        total_connections = sum(stats['connections'] for stats in self.backend_stats.values())
        total_served = sum(stats['total_served'] for stats in self.backend_stats.values())
        healthy_backends = sum(1 for stats in self.backend_stats.values() if stats['healthy'])
        uptime = time.time() - self.start_time
        balancer_connections = len(self.active_connections)
        
        # Calcular estadísticas de distribución
        conn_counts = [stats['connections'] for stats in self.backend_stats.values() if stats['connections'] > 0]
        if conn_counts:
            avg_connections = sum(conn_counts) / len(conn_counts)
            max_connections = max(conn_counts)
            min_connections = min(conn_counts)
            balance_ratio = (min_connections / max_connections * 100) if max_connections > 0 else 100
        else:
            avg_connections = 0
            max_connections = 0
            min_connections = 0
            balance_ratio = 100

        logger.info("=" * 80)
        logger.info(f"📊 ESTADÍSTICAS DEL BALANCEADOR")
        logger.info(f"📊 Balanceador (8080): {balancer_connections} conexiones activas")
        logger.info(f"📊 Backends: {total_connections} conexiones distribuidas en {len(conn_counts)} puertos")
        logger.info(f"📊 Total servidas: {total_served} conexiones (uptime: {uptime/3600:.1f}h)")
        logger.info(f"📊 Backends saludables: {healthy_backends}/{len(VOSK_PORTS)}")
        logger.info(f"📊 Balance: {balance_ratio:.1f}% (min={min_connections}, max={max_connections}, avg={avg_connections:.1f})")

        # Top 10 puertos más cargados
        busy_ports = sorted(
            [(port, stats) for port, stats in self.backend_stats.items() if stats['connections'] > 0],
            key=lambda x: x[1]['connections'],
            reverse=True
        )[:10]
        
        if busy_ports:
            logger.info(f"📊 Top 10 puertos más cargados:")
            for port, stats in busy_ports:
                health_status = "✅" if stats['healthy'] else "❌"
                logger.info(
                    f"   Puerto {port}: {stats['connections']} conn actuales, "
                    f"{stats['total_served']} total, "
                    f"avg_dur: {stats['avg_duration']:.1f}s, "
                    f"resp: {stats['response_time']:.0f}ms, "
                    f"err: {stats['errors']} {health_status}"
                )
        
        # Puertos con errores
        error_ports = [(port, stats['errors']) for port, stats in self.backend_stats.items() if stats['errors'] > 0]
        if error_ports:
            logger.warning(f"⚠️ Puertos con errores:")
            for port, errors in sorted(error_ports, key=lambda x: x[1], reverse=True)[:5]:
                logger.warning(f"   Puerto {port}: {errors} errores")
        
        # Resetear contador de errores cada ciclo de stats (para que se recuperen)
        for stats in self.backend_stats.values():
            if stats['errors'] > 0:
                stats['errors'] = max(0, stats['errors'] - 1)  # Decrementar gradualmente
                if stats['errors'] == 0 and not stats['healthy']:
                    stats['healthy'] = True  # Recuperar si ya no hay errores
                    logger.info(f"✅ Puerto recuperado de errores")
        
        logger.info("=" * 80)
    
    async def monitor_balance_health(self):
        """Monitorear la salud del balanceo cada 30 segundos"""
        while True:
            try:
                await asyncio.sleep(30)
                
                # Calcular métricas de balanceo
                active_ports = [
                    (port, stats['connections']) 
                    for port, stats in self.backend_stats.items() 
                    if stats['connections'] > 0
                ]
                
                if len(active_ports) < 2:
                    continue  # No hay suficientes puertos para evaluar balance
                
                conn_counts = [count for _, count in active_ports]
                max_conn = max(conn_counts)
                min_conn = min(conn_counts)
                avg_conn = sum(conn_counts) / len(conn_counts)
                std_dev = (sum((x - avg_conn) ** 2 for x in conn_counts) / len(conn_counts)) ** 0.5
                
                # Evaluar calidad del balanceo
                imbalance = max_conn - min_conn
                
                if imbalance > 15:
                    logger.warning(
                        f"⚖️ DESBALANCEO ALTO: diferencia de {imbalance} conexiones "
                        f"(min={min_conn}, max={max_conn}, std={std_dev:.1f})"
                    )
                elif imbalance > 8:
                    logger.info(
                        f"⚖️ Desbalanceo moderado: diferencia de {imbalance} conexiones "
                        f"(min={min_conn}, max={max_conn})"
                    )
                else:
                    logger.debug(
                        f"✅ Buen balance: diferencia de {imbalance} conexiones "
                        f"(min={min_conn}, max={max_conn})"
                    )
                
            except Exception as e:
                logger.error(f"Error en monitor de balance: {e}")
    
    def get_balance_report(self):
        """Obtener reporte detallado del estado de balanceo"""
        report = {
            'total_backends': len(VOSK_PORTS),
            'healthy_backends': sum(1 for s in self.backend_stats.values() if s['healthy']),
            'active_backends': sum(1 for s in self.backend_stats.values() if s['connections'] > 0),
            'total_connections': sum(s['connections'] for s in self.backend_stats.values()),
            'total_served': sum(s['total_served'] for s in self.backend_stats.values()),
            'ports': []
        }
        
        for port, stats in sorted(self.backend_stats.items(), key=lambda x: x[1]['connections'], reverse=True):
            if stats['connections'] > 0 or stats['total_served'] > 0:
                report['ports'].append({
                    'port': port,
                    'connections': stats['connections'],
                    'total_served': stats['total_served'],
                    'avg_duration': round(stats['avg_duration'], 2),
                    'response_time': round(stats['response_time'], 1),
                    'errors': stats['errors'],
                    'healthy': stats['healthy']
                })
        
        return report
    
    async def verify_connection_counts(self):
        """Verificar y corregir inconsistencias en los contadores de conexiones"""
        # Contar conexiones reales por puerto
        real_counts = defaultdict(int)
        for conn_info in self.active_connections.values():
            if conn_info.get('backend_port'):
                real_counts[conn_info['backend_port']] += 1
        
        # Comparar con los contadores almacenados
        corrected = False
        for port in VOSK_PORTS:
            real_count = real_counts.get(port, 0)
            stored_count = self.backend_stats[port]['connections']
            
            if real_count != stored_count:
                logger.warning(
                    f"⚠️ INCONSISTENCIA Puerto {port}: "
                    f"Contador={stored_count} Real={real_count} - CORRIGIENDO"
                )
                self.backend_stats[port]['connections'] = real_count
                corrected = True
        
        if corrected:
            logger.info("✅ Contadores de conexiones corregidos")
    
    def reset_port_counter(self, port):
        """Resetear manualmente el contador de un puerto específico"""
        if port in self.backend_stats:
            old_count = self.backend_stats[port]['connections']
            # Contar conexiones reales
            real_count = sum(1 for conn in self.active_connections.values() 
                           if conn.get('backend_port') == port)
            self.backend_stats[port]['connections'] = real_count
            logger.info(f"🔄 Puerto {port} reseteado: {old_count} -> {real_count}")
            return real_count
        return None
    
    def reset_all_counters(self):
        """Resetear todos los contadores basándose en conexiones reales"""
        logger.info("🔄 Reseteando todos los contadores...")
        real_counts = defaultdict(int)
        for conn_info in self.active_connections.values():
            if conn_info.get('backend_port'):
                real_counts[conn_info['backend_port']] += 1
        
        for port in VOSK_PORTS:
            old_count = self.backend_stats[port]['connections']
            new_count = real_counts.get(port, 0)
            self.backend_stats[port]['connections'] = new_count
            if old_count != new_count:
                logger.info(f"   Puerto {port}: {old_count} -> {new_count}")
        
        logger.info("✅ Todos los contadores reseteados")

    def get_best_backend(self):
        """Seleccionar el mejor backend usando algoritmo de balanceo inteligente"""
        current_time = time.time()
        
        # Filtrar backends saludables y que NO estén llenos
        healthy_backends = [
            port for port, stats in self.backend_stats.items()
            if stats['healthy'] and stats['connections'] < MAX_CONNECTIONS_PER_PORT
        ]

        if not healthy_backends:
            logger.warning(f"⚠️ No hay backends saludables disponibles con espacio")
            # Si no hay backends saludables, usar cualquiera disponible
            available_backends = [
                port for port, stats in self.backend_stats.items()
                if stats['connections'] < MAX_CONNECTIONS_PER_PORT
            ]
            if available_backends:
                selected = random.choice(available_backends)
                logger.info(f"⚠️ Usando backend no-saludable {selected}")
                return selected
            else:
                # CASO CRÍTICO: Todos los puertos reportan estar llenos
                logger.error("🚨 TODOS LOS PUERTOS REPORTAN ESTAR LLENOS - Verificando contadores...")
                
                # Contar conexiones reales
                real_counts = defaultdict(int)
                for conn_info in self.active_connections.values():
                    if conn_info.get('backend_port'):
                        real_counts[conn_info['backend_port']] += 1
                
                # Buscar puertos con contadores incorrectos
                for port in VOSK_PORTS:
                    real_count = real_counts.get(port, 0)
                    if real_count < MAX_CONNECTIONS_PER_PORT:
                        logger.warning(
                            f"⚠️ Puerto {port} tiene contador incorrecto: "
                            f"reportado={self.backend_stats[port]['connections']}, real={real_count}"
                        )
                        self.backend_stats[port]['connections'] = real_count
                        return port
                
                # En último caso extremo, usar el que tenga menos conexiones REALES
                if real_counts:
                    selected = min(real_counts.keys(), key=lambda p: real_counts[p])
                else:
                    selected = min(self.backend_stats.keys(),
                                 key=lambda p: self.backend_stats[p]['connections'])
                
                logger.error(f"🚨 Forzando asignación a puerto {selected}")
                return selected

        # ALGORITMO DE BALANCEO INTELIGENTE
        # Calcular score para cada backend basado en múltiples factores
        
        def calculate_backend_score(port):
            """
            Calcular score de un backend (menor es mejor)
            Factores:
            1. Número de conexiones actuales (peso alto)
            2. Tasa de errores recientes
            3. Tiempo desde última asignación (para distribuir uniformemente)
            4. Capacidad disponible
            """
            stats = self.backend_stats[port]
            
            # Factor 1: Carga actual (0-100 puntos)
            # Penalizar proporcionalmente al número de conexiones
            load_score = (stats['connections'] / MAX_CONNECTIONS_PER_PORT) * 100
            
            # Factor 2: Errores recientes (0-50 puntos)
            # Penalizar puertos con errores
            error_score = min(stats['errors'] * 10, 50)
            
            # Factor 3: Distribución temporal (0-30 puntos)
            # Preferir puertos que no se han usado recientemente
            time_since_last = current_time - stats['last_assigned']
            if time_since_last > 10:  # Más de 10 segundos sin usar
                time_score = 0  # Bonus: usar este puerto
            elif time_since_last > 5:
                time_score = 10
            elif time_since_last > 1:
                time_score = 20
            else:
                time_score = 30  # Penalizar si se acaba de usar
            
            # Factor 4: Salud general (0-20 puntos)
            # Basado en tiempo de respuesta y disponibilidad
            health_score = 0 if stats['response_time'] < 100 else 20
            
            total_score = load_score + error_score + time_score + health_score
            
            return total_score
        
        # Calcular scores para todos los backends saludables
        backend_scores = [(port, calculate_backend_score(port)) for port in healthy_backends]
        
        # Ordenar por score (menor es mejor)
        backend_scores.sort(key=lambda x: x[1])
        
        # Tomar los top 5 mejores backends
        top_backends = [port for port, score in backend_scores[:5]]
        
        # De los top 5, elegir el que tiene menos conexiones
        # (esto evita siempre elegir el mismo y distribuye mejor)
        selected = min(top_backends, key=lambda p: self.backend_stats[p]['connections'])
        
        # Log detallado si hay desbalanceo significativo
        conn_counts = [self.backend_stats[p]['connections'] for p in healthy_backends]
        if conn_counts:
            max_conn = max(conn_counts)
            min_conn = min(conn_counts)
            if max_conn - min_conn > 10:  # Desbalanceo > 10 conexiones
                logger.info(
                    f"⚖️ Desbalanceo detectado: min={min_conn}, max={max_conn}, "
                    f"seleccionado={selected} con {self.backend_stats[selected]['connections']} conexiones"
                )
        
        # Actualizar timestamp de última asignación
        self.backend_stats[selected]['last_assigned'] = current_time
        
        # Log si está cerca del límite
        if self.backend_stats[selected]['connections'] > MAX_CONNECTIONS_PER_PORT * 0.8:
            logger.warning(
                f"⚠️ Puerto {selected} al {self.backend_stats[selected]['connections']/MAX_CONNECTIONS_PER_PORT*100:.0f}% de capacidad"
            )
        
        return selected

    async def proxy_websocket(self, client_ws, path):
        """Manejar proxy de WebSocket con mejor gestión de conexiones"""
        backend_port = None
        backend_ws = None
        client_addr = f"{client_ws.remote_address[0]}:{client_ws.remote_address[1]}"
        conn_id = f"{client_addr}_{int(time.time()*1000)}"
        connection_start = time.time()
        connection_success = False

        try:
            # Seleccionar backend con algoritmo inteligente
            selection_start = time.time()
            backend_port = self.get_best_backend()
            selection_time = (time.time() - selection_start) * 1000  # ms
            
            backend_url = f"ws://127.0.0.1:{backend_port}{path}"

            logger.debug(f"🔗 Nueva conexión {client_addr} -> puerto {backend_port} (selección: {selection_time:.1f}ms)")

            # Conectar al backend y medir tiempo de respuesta
            connect_start = time.time()
            backend_ws = await asyncio.wait_for(
                websockets.connect(
                    backend_url,
                    max_size=None,
                    ping_interval=None,  # Deshabilitar ping automático
                    ping_timeout=None,
                    close_timeout=5
                ),
                timeout=3
            )
            connect_time = (time.time() - connect_start) * 1000  # ms
            
            # Actualizar tiempo de respuesta promedio del backend
            stats = self.backend_stats[backend_port]
            if stats['response_time'] == 0:
                stats['response_time'] = connect_time
            else:
                # Promedio móvil exponencial (70% histórico, 30% nuevo)
                stats['response_time'] = stats['response_time'] * 0.7 + connect_time * 0.3

            # Registrar conexión ANTES de incrementar contador
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
                f"total: {self.backend_stats[backend_port]['total_served']}, "
                f"tiempo: {connect_time:.1f}ms)"
            )

            # Crear tareas para proxy bidireccional con mejor manejo de errores
            try:
                await asyncio.gather(
                    self.forward_messages(client_ws, backend_ws, f"{client_addr}->B{backend_port}", conn_id),
                    self.forward_messages(backend_ws, client_ws, f"B{backend_port}->{client_addr}", conn_id),
                    return_exceptions=True
                )
            except Exception as e:
                logger.debug(f"Error en proxy bidireccional: {e}")
                self.backend_stats[backend_port]['errors'] += 1

        except asyncio.TimeoutError:
            logger.warning(f"❌ Timeout conectando a backend {backend_port} para {client_addr}")
            if backend_port:
                self.backend_stats[backend_port]['errors'] += 1
                # Marcar como no saludable temporalmente si hay muchos errores
                if self.backend_stats[backend_port]['errors'] > 5:
                    self.backend_stats[backend_port]['healthy'] = False
                    logger.error(f"🚨 Puerto {backend_port} marcado como NO SALUDABLE (errores: {self.backend_stats[backend_port]['errors']})")
        except Exception as e:
            logger.error(f"❌ Error en proxy para {client_addr}: {e}")
            if backend_port:
                self.backend_stats[backend_port]['errors'] += 1

        finally:
            # Calcular duración de la conexión
            connection_duration = time.time() - connection_start
            
            # CRÍTICO: Limpieza inmediata y SIEMPRE decrementar el contador
            conn_info = self.active_connections.pop(conn_id, None)
            if conn_info:
                await self._cleanup_connection(conn_info)
                
                # Actualizar duración promedio si la conexión fue exitosa
                if connection_success and backend_port:
                    stats = self.backend_stats[backend_port]
                    if stats['avg_duration'] == 0:
                        stats['avg_duration'] = connection_duration
                    else:
                        # Promedio móvil exponencial
                        stats['avg_duration'] = stats['avg_duration'] * 0.9 + connection_duration * 0.1
                        
            elif backend_port is not None:
                # Si no hay conn_info pero sí se asignó un puerto, decrementar manualmente
                self.backend_stats[backend_port]['connections'] = max(
                    0, self.backend_stats[backend_port]['connections'] - 1
                )
                logger.debug(f"⚠️ Decrementado manualmente puerto {backend_port}")

            if backend_port:
                logger.info(
                    f"🔌 Conexión cerrada {client_addr} "
                    f"(puerto {backend_port} ahora: {self.backend_stats[backend_port]['connections']}, "
                    f"duración: {connection_duration:.1f}s)"
                )

    async def forward_messages(self, source, destination, direction, conn_id):
        """Reenviar mensajes entre WebSockets con mejor gestión de errores"""
        try:
            async for message in source:
                # Verificar que la conexión siga activa
                if conn_id not in self.active_connections:
                    break

                if destination.closed:
                    break

                await destination.send(message)

        except websockets.exceptions.ConnectionClosed:
            logger.debug(f"Conexión cerrada en {direction}")
        except Exception as e:
            logger.debug(f"Error forwarding {direction}: {e}")
        finally:
            # Asegurar que ambas conexiones se cierren
            try:
                if not source.closed:
                    await source.close()
                if not destination.closed:
                    await destination.close()
            except Exception:
                pass

# Instancia global del balanceador
balancer = LoadBalancer()

async def handle_websocket(websocket, path):
    """Handler principal para conexiones WebSocket"""
    await balancer.proxy_websocket(websocket, path)

async def main():
    """Función principal del balanceador"""
    logger.info(f"🚀 Iniciando balanceador Python en puerto {LISTEN_PORT}")
    logger.info(f"📡 Balanceando entre puertos {VOSK_PORTS[0]}-{VOSK_PORTS[-1]} ({len(VOSK_PORTS)} backends)")
    logger.info(f"📡 Capacidad máxima: {len(VOSK_PORTS)} × {MAX_CONNECTIONS_PER_PORT} = {len(VOSK_PORTS) * MAX_CONNECTIONS_PER_PORT} conexiones")
    logger.info(f"⚙️ Algoritmo: Balanceo inteligente multi-factor")

    # Iniciar tareas de mantenimiento
    health_task = asyncio.create_task(balancer.health_check())
    cleanup_task = asyncio.create_task(balancer.cleanup_stale_connections())
    monitor_task = asyncio.create_task(balancer.monitor_balance_health())

    # Configurar manejador de señales
    def signal_handler():
        logger.info("🛑 Deteniendo balanceador...")
        health_task.cancel()
        cleanup_task.cancel()
        monitor_task.cancel()

    # Iniciar servidor WebSocket con configuración optimizada
    async with websockets.serve(
        handle_websocket,
        "0.0.0.0",
        LISTEN_PORT,
        ping_interval=None,  # Deshabilitar ping automático del balanceador
        ping_timeout=None,
        close_timeout=5,     # Timeout corto para cerrar conexiones
        max_size=None,
        compression=None,
        max_queue=100        # Limitar cola de mensajes
    ):
        try:
            await asyncio.Future()  # Ejecutar indefinidamente
        except KeyboardInterrupt:
            signal_handler()
        finally:
            # Limpiar todas las conexiones activas
            logger.info("🧹 Limpiando conexiones activas...")
            for conn_info in list(balancer.active_connections.values()):
                await balancer._cleanup_connection(conn_info)
            balancer.active_connections.clear()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("💀 Balanceador detenido por el usuario")
    except Exception as e:
        logger.error(f"💥 Error crítico: {e}")
        sys.exit(1)