const WebSocket = require('ws');

const ws = new WebSocket('ws://tvnovedades.bestvoiper.com:8766');

// Esperar a que la conexión esté abierta
ws.on('open', () => {
  console.log('✅ Conectado al servidor WebSocket');
  
  // Ahora sí podemos enviar mensajes
  ws.send(JSON.stringify({
    type: 'get_prediction',
    queue_name: 'ventas',
    minutes_ahead: 10
  }));
});

ws.on('message', (data) => {
  const message = JSON.parse(data.toString());
  console.log('\n📨 Mensaje recibido:', message.type);
  
  if (message.type === 'queue_update') {
    console.log('📊 Colas:', JSON.stringify(message.data.queues, null, 2));
    if (message.data.analytics?.anomalies?.length > 0) {
      console.log('⚠️ Anomalías:', message.data.analytics.anomalies);
    }
    if (message.data.analytics?.suggestions?.length > 0) {
      console.log('💡 Sugerencias:', message.data.analytics.suggestions);
    }
  } else if (message.type === 'initial_data') {
    console.log('📋 Datos iniciales recibidos');
    console.log('   Total colas:', message.data.total_queues);
    console.log('   Llamadas en espera:', message.data.total_waiting);
    console.log('   Agentes disponibles:', message.data.total_available);
  } else if (message.type === 'prediction') {
    console.log('🔮 Predicción:', JSON.stringify(message.data, null, 2));
  }
});

ws.on('error', (error) => {
  console.error('❌ Error WebSocket:', error.message);
});

ws.on('close', () => {
  console.log('🔌 Conexión cerrada');
});

// Mantener el proceso vivo
console.log('🚀 Conectando a ws://localhost:8766...');