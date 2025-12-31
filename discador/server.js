const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const PORT = 3000;
const WS_TARGET = 'ws://tvnovedades.bestvoiper.com:8766';

// Colores para la consola
const colors = {
    reset: '\x1b[0m',
    bright: '\x1b[1m',
    dim: '\x1b[2m',
    red: '\x1b[31m',
    green: '\x1b[32m',
    yellow: '\x1b[33m',
    blue: '\x1b[34m',
    magenta: '\x1b[35m',
    cyan: '\x1b[36m',
    white: '\x1b[37m',
    bgRed: '\x1b[41m',
    bgGreen: '\x1b[42m',
    bgYellow: '\x1b[43m',
    bgBlue: '\x1b[44m',
};

function timestamp() {
    return new Date().toLocaleTimeString('es-ES', { 
        hour: '2-digit', 
        minute: '2-digit', 
        second: '2-digit',
        hour12: false 
    });
}

function log(type, message, data = null) {
    const ts = `${colors.dim}[${timestamp()}]${colors.reset}`;
    
    switch(type) {
        case 'info':
            console.log(`${ts} ${colors.blue}ℹ️  INFO${colors.reset} ${message}`);
            break;
        case 'success':
            console.log(`${ts} ${colors.green}✅ SUCCESS${colors.reset} ${message}`);
            break;
        case 'warning':
            console.log(`${ts} ${colors.yellow}⚠️  WARNING${colors.reset} ${message}`);
            break;
        case 'error':
            console.log(`${ts} ${colors.red}❌ ERROR${colors.reset} ${message}`);
            break;
        case 'ws-in':
            console.log(`${ts} ${colors.cyan}📥 WS-IN${colors.reset} ${message}`);
            break;
        case 'ws-out':
            console.log(`${ts} ${colors.magenta}📤 WS-OUT${colors.reset} ${message}`);
            break;
        case 'prediction':
            console.log(`${ts} ${colors.bright}${colors.magenta}🔮 PREDICTION${colors.reset} ${message}`);
            break;
        case 'queue':
            console.log(`${ts} ${colors.green}📊 QUEUE${colors.reset} ${message}`);
            break;
        case 'anomaly':
            console.log(`${ts} ${colors.bgRed}${colors.white}⚠️  ANOMALY${colors.reset} ${message}`);
            break;
    }
    
    if (data) {
        console.log(`${colors.dim}    └─ ${JSON.stringify(data, null, 2).split('\n').join('\n       ')}${colors.reset}`);
    }
}

// Servidor HTTP para archivos estáticos
const httpServer = http.createServer((req, res) => {
    let filePath = path.join(__dirname, req.url === '/' ? 'dashboard.html' : req.url);
    
    const extname = path.extname(filePath);
    let contentType = 'text/html';
    
    switch (extname) {
        case '.js': contentType = 'text/javascript'; break;
        case '.css': contentType = 'text/css'; break;
        case '.json': contentType = 'application/json'; break;
        case '.png': contentType = 'image/png'; break;
        case '.jpg': contentType = 'image/jpg'; break;
    }
    
    fs.readFile(filePath, (error, content) => {
        if (error) {
            if (error.code === 'ENOENT') {
                res.writeHead(404);
                res.end('Archivo no encontrado');
                log('warning', `404 - ${req.url}`);
            } else {
                res.writeHead(500);
                res.end('Error del servidor: ' + error.code);
                log('error', `500 - ${req.url} - ${error.code}`);
            }
        } else {
            res.writeHead(200, { 'Content-Type': contentType });
            res.end(content, 'utf-8');
            if (req.url !== '/favicon.ico') {
                log('info', `HTTP: ${req.method} ${req.url}`);
            }
        }
    });
});

// Servidor WebSocket Proxy
const wss = new WebSocket.Server({ server: httpServer });

wss.on('connection', (clientWs, req) => {
    const clientIp = req.socket.remoteAddress;
    log('success', `Cliente conectado: ${clientIp}`);
    
    // Conectar al servidor WebSocket real
    const targetWs = new WebSocket(WS_TARGET);
    
    targetWs.on('open', () => {
        log('success', `Conectado al servidor destino: ${WS_TARGET}`);
    });
    
    targetWs.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            
            // Procesar y mostrar según el tipo de mensaje
            switch(message.type) {
                case 'initial_data':
                    log('ws-in', `INITIAL_DATA - ${message.data?.total_queues || 0} colas`);
                    if (message.data?.queues) {
                        message.data.queues.forEach(q => {
                            log('queue', `  Cola: ${q.name} | Espera: ${q.calls_waiting} | Disponibles: ${q.members?.available} | Ocupados: ${q.members?.busy}`);
                        });
                    }
                    break;
                    
                case 'queue_update':
                    const d = message.data;
                    log('ws-in', `QUEUE_UPDATE - Colas: ${d?.total_queues} | Espera: ${d?.total_waiting} | Disponibles: ${d?.total_available} | Ocupados: ${d?.total_busy}`);
                    
                    // Mostrar detalles de cada cola
                    if (d?.queues && d.queues.length > 0) {
                        d.queues.forEach(q => {
                            const status = q.calls_waiting > 5 ? '🔴' : q.calls_waiting > 0 ? '🟡' : '🟢';
                            log('queue', `  ${status} ${q.name}: ${q.calls_waiting} espera, ${q.members?.available || 0} disp, ${q.members?.busy || 0} ocup, ${q.completed || 0} comp, ${q.abandoned || 0} aband`);
                        });
                    }
                    
                    // Mostrar anomalías
                    if (d?.analytics?.anomalies && d.analytics.anomalies.length > 0) {
                        d.analytics.anomalies.forEach(a => {
                            log('anomaly', `${a.queue}: ${a.message} (${a.severity})`);
                        });
                    }
                    
                    // Mostrar sugerencias
                    if (d?.analytics?.suggestions && d.analytics.suggestions.length > 0) {
                        d.analytics.suggestions.forEach(s => {
                            const msg = typeof s === 'string' ? s : s.message;
                            log('info', `💡 Sugerencia: ${msg}`);
                        });
                    }
                    break;
                    
                case 'prediction':
                    const p = message.data;
                    log('prediction', `Cola: ${p.queue}`);
                    
                    // Verificar si hay mensaje de datos insuficientes
                    if (p.message) {
                        log('warning', `  ⚠️ ${p.message}`);
                        log('prediction', `  🎯 Confianza: ${p.prediction_confidence || p.confidence || 'baja'}`);
                    } else {
                        log('prediction', `  📈 Llamadas esperadas: ${p.predicted_calls_waiting ?? 'N/A'}`);
                        log('prediction', `  ⏱️  Tiempo espera: ${p.predicted_hold_time ?? 'N/A'}s`);
                        log('prediction', `  👥 Agentes disponibles: ${p.predicted_available_agents ?? 'N/A'}`);
                        log('prediction', `  📊 Tendencia: ${p.trend || 'N/A'}`);
                        log('prediction', `  🚨 Nivel alerta: ${p.alert_level || 'N/A'}`);
                        log('prediction', `  💡 Recomendación: ${p.recommendation || 'N/A'}`);
                        log('prediction', `  🎯 Confianza: ${p.confidence || 'N/A'}`);
                    }
                    break;
                
                case 'dialer_status':
                    const ds = message.data;
                    if (ds.found) {
                        const rec = ds.dialer?.recommendation || 'unknown';
                        const icon = rec === 'stop' ? '🔴' : rec === 'pause' ? '🟡' : rec === 'slow' ? '🟠' : '🟢';
                        log('ws-in', `📞 DIALER STATUS - ${ds.queue_name}`);
                        log('ws-in', `  👥 Agentes: ${ds.agents?.available || 0} disp / ${ds.agents?.busy || 0} ocup / ${ds.agents?.total || 0} total`);
                        log('ws-in', `  📞 Llamadas: ${ds.calls?.waiting || 0} espera, ${ds.calls?.abandon_rate || 0}% abandono`);
                        log('ws-in', `  ${icon} Puede marcar: ${ds.dialer?.can_dial ? 'SÍ' : 'NO'} - ${ds.dialer?.recommendation?.toUpperCase()}`);
                        log('ws-in', `  💬 ${ds.dialer?.reason || 'N/A'}`);
                        log('ws-in', `  ⚡ Velocidad: ${ds.dialer?.suggested_rate_per_min || 0} llamadas/min`);
                    } else {
                        log('warning', `📞 DIALER: Cola '${ds.queue_name}' no encontrada`);
                    }
                    break;
                
                case 'dialer_config_updated':
                    log('success', `⚙️ DIALER CONFIG actualizada para ${message.data?.queue_name}`);
                    break;
                    
                case 'error':
                    log('error', `Error del servidor: ${message.message}`);
                    break;
                    
                case 'pong':
                    log('info', 'Pong recibido');
                    break;
                    
                default:
                    log('ws-in', `Mensaje tipo: ${message.type}`, message.data);
            }
            
            // Reenviar al cliente
            if (clientWs.readyState === WebSocket.OPEN) {
                clientWs.send(data.toString());
            }
            
        } catch (e) {
            log('error', `Error parseando mensaje: ${e.message}`);
            if (clientWs.readyState === WebSocket.OPEN) {
                clientWs.send(data.toString());
            }
        }
    });
    
    targetWs.on('error', (error) => {
        log('error', `Error conexión destino: ${error.message}`);
    });
    
    targetWs.on('close', () => {
        log('warning', 'Conexión con servidor destino cerrada');
        if (clientWs.readyState === WebSocket.OPEN) {
            clientWs.close();
        }
    });
    
    // Mensajes del cliente hacia el servidor
    clientWs.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            
            switch(message.type) {
                case 'get_prediction':
                    log('ws-out', `🔮 SOLICITUD PREDICCIÓN`);
                    log('ws-out', `  Cola: ${message.queue_name}`);
                    log('ws-out', `  Minutos adelante: ${message.minutes_ahead}`);
                    break;
                
                case 'dialer_status':
                    log('ws-out', `📞 DIALER STATUS REQUEST`);
                    log('ws-out', `  Cola: ${message.queue_name}`);
                    break;
                
                case 'dialer_config':
                    log('ws-out', `⚙️ DIALER CONFIG`);
                    log('ws-out', `  Cola: ${message.queue_name}`);
                    if (message.config) {
                        log('ws-out', `  Max ratio: ${message.config.max_ratio || 'default'}`);
                        log('ws-out', `  Min agentes: ${message.config.min_agents || 'default'}`);
                        log('ws-out', `  Max espera: ${message.config.max_wait_calls || 'default'}`);
                    }
                    break;
                    
                case 'get_queues':
                    log('ws-out', 'Solicitando estado de colas');
                    break;
                    
                case 'get_analytics':
                    log('ws-out', 'Solicitando analytics');
                    break;
                    
                case 'ping':
                    log('ws-out', 'Ping enviado');
                    break;
                    
                default:
                    log('ws-out', `Mensaje tipo: ${message.type}`, message);
            }
            
            // Reenviar al servidor destino
            if (targetWs.readyState === WebSocket.OPEN) {
                targetWs.send(data.toString());
            }
            
        } catch (e) {
            log('error', `Error parseando mensaje del cliente: ${e.message}`);
            if (targetWs.readyState === WebSocket.OPEN) {
                targetWs.send(data.toString());
            }
        }
    });
    
    clientWs.on('close', () => {
        log('warning', `Cliente desconectado: ${clientIp}`);
        if (targetWs.readyState === WebSocket.OPEN) {
            targetWs.close();
        }
    });
    
    clientWs.on('error', (error) => {
        log('error', `Error cliente: ${error.message}`);
    });
});

httpServer.listen(PORT, () => {
    console.log(`
${colors.bright}${colors.cyan}
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║   📞 CALL CENTER DASHBOARD - SERVIDOR CON LOGGING                   ║
║                                                                      ║
║   🌐 Dashboard:  http://localhost:${PORT}                              ║
║   🔌 WebSocket:  ws://localhost:${PORT}                                ║
║   🎯 Target:     ${WS_TARGET}               ║
║                                                                      ║
║   📊 Logs habilitados:                                               ║
║      • 📥 Mensajes entrantes del servidor                           ║
║      • 📤 Mensajes salientes del cliente                            ║
║      • 🔮 Predicciones IA                                           ║
║      • ⚠️  Anomalías detectadas                                      ║
║      • 📋 Estado de colas                                           ║
║                                                                      ║
║   ⌨️  Presiona Ctrl+C para detener                                   ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
${colors.reset}
    `);
    
    log('success', 'Servidor iniciado correctamente');
});
