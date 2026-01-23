import subprocess
import sys
import signal
import os
import time
import threading
import socket
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

PID_FILE = "./recordings/eralyws_pids.txt"
NAME_PREFIX = "eralyws_port_"
LOG_FILE = "./recordings/subir_prts.log"

# Usa los puertos del 8101 al 8251 inclusive
PORTS = list(range(8101, 8301)) # 200 puertos a monitorear

# Configuración de monitoreo
MONITOR_INTERVAL = 30  # Segundos entre verificaciones
PORT_CHECK_TIMEOUT = 5  # Timeout para verificar puertos
MAX_RESTART_ATTEMPTS = 3  # Máximo intentos de reinicio por proceso

# Configuración de inicio paralelo
PARALLEL_WORKERS = 20  # Número de workers para inicio paralelo
STARTUP_DELAY = 0.1  # Delay mínimo entre inicios (segundos)
BATCH_SIZE = 50  # Tamaño de lote para verificación

# Contadores simples para logging
process_counters = {
    'total_started': 0,
    'total_restarts': 0,
    'port_failures': {},
    'current_processes': {}
}

def setup_logging():
    """Configurar el sistema de logging detallado"""
    try:
        os.makedirs("./recordings", exist_ok=True)
        log_file = LOG_FILE
    except (PermissionError, OSError):
        log_file = f"/tmp/subir_prts_{os.getpid()}.log"
        print(f"Warning: Using fallback log file: {log_file}")

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file) if log_file else logging.NullHandler()
        ]
    )

def log_process_detail(port, message, level=logging.INFO):
    """Log detallado específico para procesos de puertos"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    logging.log(level, f"[Puerto {port}] [{timestamp}] {message}")

def get_process_count_by_file():
    """Contar cuántos procesos maneja cada archivo relevante"""
    active_processes = len([p for p in process_counters['current_processes'].values() if p['active']])

    logging.info(f"📊 PROCESOS POR ARCHIVO:")
    logging.info(f"   📄 vosk_cli_args.py: {active_processes} procesos activos")
    logging.info(f"   📈 Total iniciados en sesión: {process_counters['total_started']}")
    logging.info(f"   🔄 Total reiniciados: {process_counters['total_restarts']}")

    # Log detalle por puerto
    for port, data in process_counters['current_processes'].items():
        if data['active']:
            failures = process_counters['port_failures'].get(port, 0)
            logging.info(f"      Puerto {port}: PID {data['pid']}, fallos: {failures}")

def check_port_availability(port, retries=2):
    """Verificar si un puerto está activo y respondiendo con logging detallado"""
    start_time = time.time()

    for attempt in range(retries):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(PORT_CHECK_TIMEOUT)
            result = sock.connect_ex(('localhost', port))
            sock.close()

            elapsed = round(time.time() - start_time, 3)

            if result == 0:
                log_process_detail(port, f"✅ Puerto responde OK en {elapsed}s (intento {attempt + 1}/{retries})")
                return True
            else:
                log_process_detail(port, f"❌ Puerto no responde - código {result} en {elapsed}s (intento {attempt + 1}/{retries})", logging.WARNING)

            if attempt < retries - 1:
                time.sleep(1)

        except Exception as e:
            elapsed = round(time.time() - start_time, 3)
            log_process_detail(port, f"❌ Error verificando puerto en {elapsed}s: {e}", logging.ERROR)
            if attempt < retries - 1:
                time.sleep(1)

    # Si llegamos aquí, el puerto falló
    total_time = round(time.time() - start_time, 3)
    log_process_detail(port, f"💥 PUERTO CAÍDO después de {retries} intentos en {total_time}s", logging.ERROR)

    # Actualizar contadores
    if port not in process_counters['port_failures']:
        process_counters['port_failures'][port] = 0
    process_counters['port_failures'][port] += 1

    logging.error(f"🚨 PUERTO {port} CAÍDO - Total fallos: {process_counters['port_failures'][port]}")

    return False

def is_process_running(pid):
    """Verificar si un proceso está ejecutándose"""
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False

def get_safe_pid_file():
    """Obtener la ruta del archivo PID con manejo de permisos"""
    try:
        os.makedirs("./recordings", exist_ok=True)
        return PID_FILE
    except (PermissionError, OSError):
        return f"/tmp/eralyws_pids_{os.getpid()}.txt"

def get_running_processes():
    """Obtener los procesos en ejecución desde el archivo PID"""
    processes = {}
    pid_file = get_safe_pid_file()

    if os.path.exists(pid_file):
        try:
            with open(pid_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(':')
                    if len(parts) == 2:
                        port = int(parts[0])
                        pid = int(parts[1])
                        if is_process_running(pid):
                            processes[port] = pid
        except Exception as e:
            logging.error(f"Error leyendo archivo PID: {e}")
    return processes

def start_process_for_port(port):
    """Iniciar un proceso para un puerto específico con logging detallado"""
    start_time = time.time()

    try:
        if not os.path.exists("vosk_cli_args.py"):
            error_msg = "❌ El archivo vosk_cli_args.py no existe en el directorio actual"
            logging.error(error_msg)
            log_process_detail(port, error_msg, logging.ERROR)
            return None

        cmd = ["python3", "vosk_cli_args.py", "--port", str(port)]
        env = os.environ.copy()
        env["ERALYWS_NAME"] = f"{NAME_PREFIX}{port}"
        env["WEBSOCKET_PORT"] = str(port)

        log_process_detail(port, f"🚀 Iniciando proceso con comando: {' '.join(cmd)}")

        process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid if hasattr(os, 'setsid') else None
        )

        time.sleep(0.3)  # Reducido para inicio más rápido

        if process.poll() is None:
            startup_time = round(time.time() - start_time, 3)
            process_counters['total_started'] += 1

            # Registrar proceso activo
            process_counters['current_processes'][port] = {
                'pid': process.pid,
                'active': True,
                'startup_time': startup_time,
                'start_timestamp': datetime.now().isoformat()
            }

            log_process_detail(port, f"✅ PROCESO INICIADO - PID {process.pid}, tiempo startup: {startup_time}s")
            logging.info(f"✅ Proceso iniciado para puerto {port} con PID {process.pid} (startup: {startup_time}s)")

            return process.pid
        else:
            stdout, stderr = process.communicate()
            startup_time = round(time.time() - start_time, 3)
            error_msg = stderr.decode() if stderr else "Sin error específico"

            log_process_detail(port, f"💥 FALLO AL INICIAR en {startup_time}s: {error_msg}", logging.ERROR)
            logging.error(f"❌ El proceso para puerto {port} falló al iniciar: {error_msg}")
            return None

    except Exception as e:
        startup_time = round(time.time() - start_time, 3)
        log_process_detail(port, f"💥 EXCEPCIÓN AL INICIAR en {startup_time}s: {e}", logging.ERROR)
        logging.error(f"❌ Error iniciando proceso para puerto {port}: {e}")
        return None

def kill_process_by_pid(pid):
    """Terminar un proceso por su PID con logging detallado"""
    try:
        if not is_process_running(pid):
            return True

        os.kill(pid, signal.SIGTERM)
        time.sleep(3)

        if is_process_running(pid):
            os.kill(pid, signal.SIGKILL)
            time.sleep(1)

        if not is_process_running(pid):
            logging.info(f"Proceso PID {pid} terminado")
            return True

    except Exception as e:
        logging.error(f"Error terminando proceso PID {pid}: {e}")
    return False

def update_pid_file(port, pid):
    """Actualizar el archivo PID con un nuevo proceso"""
    processes = {}
    pid_file = get_safe_pid_file()

    if os.path.exists(pid_file):
        try:
            with open(pid_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(':')
                    if len(parts) == 2:
                        p = int(parts[0])
                        pid_val = int(parts[1])
                        if p != port:
                            processes[p] = pid_val
        except Exception as e:
            logging.error(f"Error leyendo archivo PID para actualización: {e}")

    processes[port] = pid

    try:
        with open(pid_file, 'w') as f:
            for p, pid_val in sorted(processes.items()):
                f.write(f"{p}:{pid_val}\n")
    except Exception as e:
        logging.error(f"Error escribiendo archivo PID: {e}")

def remove_port_from_pid_file(port):
    """Remover un puerto del archivo PID"""
    pid_file = get_safe_pid_file()
    if not os.path.exists(pid_file):
        return

    processes = {}
    try:
        with open(pid_file, 'r') as f:
            for line in f:
                parts = line.strip().split(':')
                if len(parts) == 2:
                    p = int(parts[0])
                    pid_val = int(parts[1])
                    if p != port:
                        processes[p] = pid_val

        with open(pid_file, 'w') as f:
            for p, pid_val in sorted(processes.items()):
                f.write(f"{p}:{pid_val}\n")
    except Exception as e:
        logging.error(f"Error actualizando archivo PID: {e}")

class ProcessMonitor:
    """Monitor de procesos simplificado con logging detallado"""

    def __init__(self):
        self.restart_counts = {port: 0 for port in PORTS}
        self.last_check_time = {port: 0 for port in PORTS}
        self.continuous_failures = {port: 0 for port in PORTS}
        self.monitoring = False
        self.monitor_thread = None

    def start_monitoring(self):
        """Iniciar el monitoreo"""
        if self.monitoring:
            return

        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()

        logging.info("🔍 Monitor de procesos iniciado")
        logging.info(f"📊 Verificando cada {MONITOR_INTERVAL} segundos")

    def stop_monitoring(self):
        """Detener el monitoreo"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logging.info("Monitor de procesos detenido")

    def _monitor_loop(self):
        """Bucle principal de monitoreo"""
        while self.monitoring:
            try:
                self._check_and_restart_processes()
                time.sleep(MONITOR_INTERVAL)
            except Exception as e:
                logging.error(f"❌ Error en bucle de monitoreo: {e}")
                time.sleep(MONITOR_INTERVAL)

    def _check_and_restart_processes(self):
        """Verificar y reiniciar procesos caídos"""
        current_time = time.time()
        running_processes = get_running_processes()

        active_count = 0
        failed_count = 0

        for port in PORTS:
            process_alive = port in running_processes
            pid = running_processes.get(port)

            time_since_last_check = current_time - self.last_check_time[port]
            if time_since_last_check < 8:
                continue

            port_active = check_port_availability(port, retries=2)
            self.last_check_time[port] = current_time

            needs_restart = False
            reason = ""

            if not process_alive:
                needs_restart = True
                reason = "proceso no encontrado"
                self.continuous_failures[port] += 1
                failed_count += 1

            elif not port_active:
                needs_restart = True
                reason = f"puerto no responde (PID {pid})"
                self.continuous_failures[port] += 1
                failed_count += 1

                logging.warning(f"Puerto {port} tiene proceso (PID {pid}) pero no responde")
                kill_process_by_pid(pid)
                remove_port_from_pid_file(port)
            else:
                active_count += 1
                if self.continuous_failures[port] > 0:
                    log_process_detail(port, f"✅ Recuperado después de {self.continuous_failures[port]} fallos")
                    self.continuous_failures[port] = 0
                continue

            if needs_restart:
                if self.restart_counts[port] >= MAX_RESTART_ATTEMPTS:
                    log_process_detail(port, f"🚫 Superó límite de reintentos ({MAX_RESTART_ATTEMPTS})", logging.ERROR)
                    continue

                restart_delay = min(2 ** self.continuous_failures[port], 30)

                log_process_detail(port, f"🔄 Reiniciando - {reason} (intento {self.restart_counts[port] + 1}/{MAX_RESTART_ATTEMPTS})")
                logging.warning(f"🔄 Reiniciando puerto {port} - {reason}")

                process_counters['total_restarts'] += 1

                if restart_delay > 2:
                    time.sleep(restart_delay)

                new_pid = start_process_for_port(port)

                if new_pid:
                    update_pid_file(port, new_pid)
                    self.restart_counts[port] += 1

                    time.sleep(8)

                    if check_port_availability(port, retries=3):
                        log_process_detail(port, f"✅ Reiniciado exitosamente (PID {new_pid})")
                        logging.info(f"✅ Puerto {port} reiniciado exitosamente (PID {new_pid})")
                        self.restart_counts[port] = 0
                        self.continuous_failures[port] = 0
                    else:
                        log_process_detail(port, f"❌ Reiniciado pero no responde", logging.ERROR)
                else:
                    log_process_detail(port, f"❌ No se pudo reiniciar", logging.ERROR)
                    self.restart_counts[port] += 1

    def get_status_summary(self):
        """Obtener resumen del estado"""
        running_processes = get_running_processes()
        active_count = 0
        failed_count = 0

        for port in PORTS:
            process_alive = port in running_processes
            port_active = check_port_availability(port, retries=1)

            if process_alive and port_active:
                active_count += 1
            else:
                failed_count += 1

        logging.info(f"📊 Estado actual: {active_count} activos, {failed_count} con problemas")

        if failed_count > 0:
            failed_ports = []
            for port in PORTS:
                if port not in running_processes or not check_port_availability(port, retries=1):
                    failed_ports.append(port)
                    total_failures = process_counters['port_failures'].get(port, 0)
                    log_process_detail(port, f"❌ Puerto con problemas - Total fallos: {total_failures}")

            logging.warning(f"🚨 Puertos con problemas: {failed_ports}")

        return active_count, failed_count

def start_single_port(port, index, total):
    """Iniciar un solo puerto (para uso con ThreadPoolExecutor)"""
    try:
        pid = start_process_for_port(port)
        if pid:
            return (port, pid, True)
        else:
            return (port, None, False)
    except Exception as e:
        logging.error(f"❌ Error iniciando puerto {port}: {e}")
        return (port, None, False)

def start_ports():
    """Iniciar todos los procesos de puertos EN PARALELO"""
    processes = {}
    failed_ports = []
    
    start_time = time.time()
    total_ports = len(PORTS)
    
    logging.info(f"🚀 Iniciando {total_ports} puertos en PARALELO con {PARALLEL_WORKERS} workers...")
    
    # Lock para acceso thread-safe a los diccionarios
    lock = threading.Lock()
    completed = [0]  # Usar lista para poder modificar en closure
    
    def start_with_progress(port):
        result = start_single_port(port, 0, total_ports)
        with lock:
            completed[0] += 1
            if completed[0] % 20 == 0 or completed[0] == total_ports:
                elapsed = time.time() - start_time
                rate = completed[0] / elapsed if elapsed > 0 else 0
                logging.info(f"⏳ Progreso: {completed[0]}/{total_ports} ({rate:.1f} puertos/seg)")
        return result
    
    # Usar ThreadPoolExecutor para inicio paralelo
    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        futures = {executor.submit(start_with_progress, port): port for port in PORTS}
        
        for future in as_completed(futures):
            port, pid, success = future.result()
            if success and pid:
                processes[port] = pid
            else:
                failed_ports.append(port)
    
    elapsed_time = time.time() - start_time
    
    # Guardar PIDs
    pid_file = get_safe_pid_file()
    try:
        with open(pid_file, "w") as f:
            for port, pid in sorted(processes.items()):
                f.write(f"{port}:{pid}\n")
        logging.info(f"📝 PIDs guardados en: {pid_file}")
    except Exception as e:
        logging.error(f"❌ Error guardando PIDs: {e}")

    if failed_ports:
        logging.warning(f"⚠️ No se pudieron iniciar {len(failed_ports)} puertos: {failed_ports[:10]}...")

    logging.info(f"✅ Lanzados {len(processes)} de {total_ports} procesos en {elapsed_time:.1f} segundos")
    logging.info(f"⚡ Velocidad: {len(processes)/elapsed_time:.1f} puertos/segundo")

    # Verificar conectividad en paralelo
    logging.info("🔍 Verificando conectividad de puertos en paralelo...")
    time.sleep(2)  # Reducido de 5 a 2 segundos

    responsive_ports = []
    unresponsive_ports = []
    
    def check_port_quick(port):
        return (port, check_port_availability(port, retries=2))
    
    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        check_futures = {executor.submit(check_port_quick, port): port for port in processes.keys()}
        
        for future in as_completed(check_futures):
            port, is_responsive = future.result()
            if is_responsive:
                responsive_ports.append(port)
            else:
                unresponsive_ports.append(port)

    if unresponsive_ports:
        logging.warning(f"⚠️ {len(unresponsive_ports)} puertos no responden aún (se reiniciarán automáticamente)")

    if responsive_ports:
        logging.info(f"✅ Puertos respondiendo: {len(responsive_ports)}/{len(processes)}")

    return processes

def stop_ports():
    """Detener todos los procesos"""
    pid_file = get_safe_pid_file()

    if not os.path.exists(pid_file):
        logging.warning("⚠️ No se encontró archivo de PIDs")
        return

    stopped_count = 0
    try:
        with open(pid_file, "r") as f:
            for line in f:
                parts = line.strip().split(':')
                if len(parts) == 2:
                    port = int(parts[0])
                    pid = int(parts[1])
                    if kill_process_by_pid(pid):
                        stopped_count += 1
                        logging.info(f"🛑 Proceso puerto {port} (PID {pid}) detenido")
    except Exception as e:
        logging.error(f"Error deteniendo procesos: {e}")

    try:
        os.remove(pid_file)
        logging.info(f"Archivo PID eliminado")
    except Exception as e:
        logging.warning(f"No se pudo eliminar archivo PID: {e}")

    logging.info(f"🛑 {stopped_count} procesos detenidos")

def main():
    """Función principal - simplemente ejecutar y mostrar logs"""
    setup_logging()

    logging.info("🚀 INICIANDO SISTEMA DE PROCESOS")
    logging.info(f"⚙️ Configuración: {len(PORTS)} puertos ({PORTS[0]} a {PORTS[-1]})")
    logging.info(f"⚙️ Intervalo monitoreo: {MONITOR_INTERVAL}s, Max reintentos: {MAX_RESTART_ATTEMPTS}")

    # Mostrar información inicial
    get_process_count_by_file()

    # Iniciar procesos
    processes = start_ports()

    if not processes:
        logging.error("❌ FALLO CRÍTICO: No se pudo iniciar ningún proceso")
        sys.exit(1)

    # Iniciar monitoreo
    monitor = ProcessMonitor()
    monitor.start_monitoring()

    logging.info("✅ SISTEMA COMPLETAMENTE INICIADO")
    logging.info(f"🔍 Monitor automático activo - verificando cada {MONITOR_INTERVAL} segundos")
    logging.info("🔄 Los puertos caídos se reiniciarán automáticamente")
    logging.info("🛑 Presiona Ctrl+C para detener todo")
    logging.info("=" * 80)

    try:
        last_report = time.time()
        report_interval = 180  # 3 minutos

        while True:
            current_time = time.time()

            if current_time - last_report >= report_interval:
                logging.info("=" * 80)
                logging.info("📊 REPORTE PERIÓDICO DEL SISTEMA")

                active_count, failed_count = monitor.get_status_summary()
                get_process_count_by_file()

                elapsed_minutes = (current_time - last_report) / 60
                logging.info(f"⏱️ Últimos {elapsed_minutes:.1f} minutos de ejecución")

                if failed_count > len(PORTS) * 0.3:
                    logging.error(f"🚨 ALERTA CRÍTICA: {failed_count} puertos con problemas")
                elif failed_count > 0:
                    logging.warning(f"⚠️ Algunos puertos con problemas: {failed_count}/{len(PORTS)}")
                else:
                    logging.info("✅ Todos los puertos funcionando correctamente")

                logging.info("=" * 80)
                last_report = current_time

            time.sleep(30)

    except KeyboardInterrupt:
        logging.info("=" * 80)
        logging.info("🛑 DETENIENDO SISTEMA...")
        logging.info("👤 Señal de interrupción recibida")

        monitor.stop_monitoring()
        stop_ports()

        logging.info("📊 REPORTE FINAL:")
        logging.info(f"   Total procesos iniciados: {process_counters['total_started']}")
        logging.info(f"   Total reinicios: {process_counters['total_restarts']}")
        if process_counters['port_failures']:
            total_failures = sum(process_counters['port_failures'].values())
            logging.info(f"   Total fallos de puertos: {total_failures}")

        logging.info("✅ Sistema detenido completamente")
        logging.info("=" * 80)

if __name__ == "__main__":
    main()