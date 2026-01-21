"""
Monitor de Colas de Asterisk con WebSocket Server y Machine Learning
Monitorea las colas de Asterisk usando AMI y transmite via WebSocket.
Incluye análisis predictivo y optimización automática basada en patrones históricos.
"""
import asyncio
import json
import logging
import ssl
import statistics
import pickle
import os
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Set, Tuple, Any
from collections import deque
from pathlib import Path
import hashlib

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False
    print("⚠️ websockets no instalado. Ejecuta: pip install websockets")

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

try:
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    print("⚠️ sklearn no instalado. Ejecuta: pip install scikit-learn")

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuración de Asterisk AMI
AMI_HOST = "127.0.0.1"
AMI_PORT = 5038
AMI_USERNAME = "admin"
AMI_SECRET = "pbx1910jgm"

# Configuración WebSocket
WS_HOST = "0.0.0.0"
WS_PORT = 8767  # Puerto 8767 para evitar conflicto con websocket_server (8765/8766)
WS_SSL_CERT = None  # Ruta al certificado SSL para WSS
WS_SSL_KEY = None   # Ruta a la llave privada SSL

# Configuración Redis (para persistencia de métricas)
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 1

# Intervalo de monitoreo en segundos
MONITOR_INTERVAL = 3

# Configuración de Machine Learning
ML_MODEL_PATH = Path(__file__).parent / "queue_ml_model.pkl"
ML_HISTORY_SIZE = 10000  # Muestras históricas a mantener
ML_PREDICTION_WINDOW = 300  # Predicción a 5 minutos

# Configuración Q-Learning para optimización de dial rate
QL_MODEL_PATH = Path(__file__).parent / "qlearning_model.pkl"
QL_LEARNING_RATE = 0.1  # Alpha - tasa de aprendizaje
QL_DISCOUNT_FACTOR = 0.95  # Gamma - factor de descuento
QL_EXPLORATION_RATE = 0.2  # Epsilon - tasa de exploración inicial
QL_EXPLORATION_DECAY = 0.995  # Decaimiento de exploración
QL_MIN_EXPLORATION = 0.01  # Exploración mínima
ML_TRAINING_INTERVAL = 3600  # Re-entrenar cada hora


@dataclass
class QueueMember:
    """Representa un agente/miembro de la cola"""
    name: str
    interface: str
    state_interface: str = ""
    membership: str = "dynamic"
    penalty: int = 0
    calls_taken: int = 0
    last_call: int = 0
    last_pause: int = 0
    in_call: bool = False
    status: int = 0
    paused: bool = False
    paused_reason: str = ""
    ring_in_use: bool = True
    
    @property
    def status_text(self) -> str:
        """Retorna el estado en texto legible"""
        status_map = {
            0: "Desconocido",
            1: "No en uso",
            2: "En uso",
            3: "Ocupado",
            4: "Inválido",
            5: "No disponible",
            6: "Timbrando",
            7: "Timbrando en uso",
            8: "En espera"
        }
        return status_map.get(self.status, "Desconocido")
    
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "interface": self.interface,
            "status": self.status,
            "status_text": self.status_text,
            "paused": self.paused,
            "paused_reason": self.paused_reason,
            "calls_taken": self.calls_taken,
            "in_call": self.in_call,
            "last_call": self.last_call  # Timestamp de última llamada (para sobrediscado)
        }


@dataclass
class QueueCall:
    """Representa una llamada en espera en la cola"""
    position: int
    channel: str
    uniqueid: str
    caller_id_num: str
    caller_id_name: str
    connected_line_num: str = ""
    connected_line_name: str = ""
    wait: int = 0
    priority: int = 0
    
    @property
    def wait_formatted(self) -> str:
        """Retorna el tiempo de espera formateado"""
        minutes, seconds = divmod(self.wait, 60)
        return f"{minutes:02d}:{seconds:02d}"
    
    def to_dict(self) -> dict:
        return {
            "position": self.position,
            "caller_id": self.caller_id_num,
            "caller_name": self.caller_id_name,
            "wait_seconds": self.wait,
            "wait_formatted": self.wait_formatted,
            "channel": self.channel
        }


@dataclass
class QueueStats:
    """Estadísticas de una cola"""
    name: str
    max_members: int = 0
    strategy: str = "ringall"
    calls: int = 0
    holdtime: int = 0
    talk_time: int = 0
    completed: int = 0
    abandoned: int = 0
    service_level: int = 0
    service_level_perf: float = 0.0
    service_level_perf2: float = 0.0
    weight: int = 0
    members: List[QueueMember] = field(default_factory=list)
    callers: List[QueueCall] = field(default_factory=list)
    
    @property
    def available_members(self) -> int:
        """Cuenta los miembros disponibles"""
        return sum(1 for m in self.members if m.status == 1 and not m.paused)
    
    @property
    def busy_members(self) -> int:
        """Cuenta los miembros ocupados"""
        return sum(1 for m in self.members if m.status in (2, 3, 6, 7))
    
    @property
    def paused_members(self) -> int:
        """Cuenta los miembros en pausa"""
        return sum(1 for m in self.members if m.paused)
    
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "strategy": self.strategy,
            "calls_waiting": self.calls,
            "hold_time": self.holdtime,
            "talk_time": self.talk_time,
            "completed": self.completed,
            "abandoned": self.abandoned,
            "service_level": self.service_level_perf,
            "members": {
                "total": len(self.members),
                "available": self.available_members,
                "busy": self.busy_members,
                "paused": self.paused_members,
                "details": [m.to_dict() for m in self.members]
            },
            "callers": [c.to_dict() for c in self.callers]
        }


class QLearningDialOptimizer:
    """
    Q-Learning para optimización automática de la tasa de marcado.
    Aprende la mejor acción (aumentar/mantener/reducir dial rate) según el estado de la cola.
    """
    
    # Definición de estados discretos
    STATES = {
        'calls_waiting': [0, 1, 3, 5, 10],  # Rangos: 0, 1-2, 3-4, 5-9, 10+
        'available_agents': [0, 1, 2, 3, 5],  # Rangos: 0, 1, 2, 3-4, 5+
        'hold_time': [0, 30, 60, 120, 300],  # Segundos: 0-29, 30-59, 60-119, 120-299, 300+
        'abandon_rate': [0, 5, 10, 20, 50]  # Porcentaje: 0-4, 5-9, 10-19, 20-49, 50+
    }
    
    # Acciones posibles
    ACTIONS = {
        0: 'decrease_fast',   # Reducir rápidamente (-50%)
        1: 'decrease_slow',   # Reducir lentamente (-20%)
        2: 'maintain',        # Mantener actual
        3: 'increase_slow',   # Aumentar lentamente (+20%)
        4: 'increase_fast'    # Aumentar rápidamente (+50%)
    }
    
    def __init__(self):
        self.q_table: Dict[str, Dict[int, float]] = {}
        self.learning_rate = QL_LEARNING_RATE
        self.discount_factor = QL_DISCOUNT_FACTOR
        self.exploration_rate = QL_EXPLORATION_RATE
        self.exploration_decay = QL_EXPLORATION_DECAY
        self.min_exploration = QL_MIN_EXPLORATION
        self.last_state: Optional[str] = None
        self.last_action: Optional[int] = None
        self.episode_rewards: List[float] = []
        self.total_episodes = 0
        self.current_dial_rate = 1.0  # Factor multiplicador
        
        self._load_model()
    
    def _load_model(self):
        """Carga el modelo Q-Learning desde disco"""
        try:
            if QL_MODEL_PATH.exists():
                with open(QL_MODEL_PATH, 'rb') as f:
                    data = pickle.load(f)
                    self.q_table = data.get('q_table', {})
                    self.exploration_rate = data.get('exploration_rate', QL_EXPLORATION_RATE)
                    self.total_episodes = data.get('total_episodes', 0)
                    self.episode_rewards = data.get('episode_rewards', [])[-1000:]  # Últimos 1000
                    logger.info(f"✅ Q-Learning cargado: {len(self.q_table)} estados, {self.total_episodes} episodios")
        except Exception as e:
            logger.warning(f"⚠️ No se pudo cargar Q-Learning: {e}")
    
    def _save_model(self):
        """Guarda el modelo Q-Learning a disco"""
        try:
            data = {
                'q_table': self.q_table,
                'exploration_rate': self.exploration_rate,
                'total_episodes': self.total_episodes,
                'episode_rewards': self.episode_rewards[-1000:],
                'saved_at': datetime.now().isoformat()
            }
            with open(QL_MODEL_PATH, 'wb') as f:
                pickle.dump(data, f)
            logger.debug("💾 Q-Learning guardado")
        except Exception as e:
            logger.error(f"Error guardando Q-Learning: {e}")
    
    def _discretize_value(self, value: float, thresholds: List[int]) -> int:
        """Convierte un valor continuo en un índice discreto"""
        for i, threshold in enumerate(thresholds):
            if value < threshold:
                return i
        return len(thresholds)
    
    def _get_state(self, queue_stats: Dict) -> str:
        """Convierte el estado de la cola en una cadena de estado discreto"""
        calls = self._discretize_value(queue_stats.get('calls_waiting', 0), self.STATES['calls_waiting'])
        agents = self._discretize_value(queue_stats.get('available_agents', 0), self.STATES['available_agents'])
        hold = self._discretize_value(queue_stats.get('hold_time', 0), self.STATES['hold_time'])
        abandon = self._discretize_value(queue_stats.get('abandon_rate', 0), self.STATES['abandon_rate'])
        
        return f"{calls}_{agents}_{hold}_{abandon}"
    
    def _get_q_values(self, state: str) -> Dict[int, float]:
        """Obtiene los valores Q para un estado, inicializando si es necesario"""
        if state not in self.q_table:
            # Inicializar con valores que favorecen mantener
            self.q_table[state] = {a: 0.0 for a in self.ACTIONS.keys()}
            self.q_table[state][2] = 0.1  # Pequeño bonus para 'maintain'
        return self.q_table[state]
    
    def _calculate_reward(self, prev_stats: Dict, curr_stats: Dict) -> float:
        """
        Calcula la recompensa basada en cambios en métricas.
        Recompensa positiva: mejora en métricas
        Recompensa negativa: empeoramiento
        """
        reward = 0.0
        
        # Cambio en llamadas en espera (menos es mejor)
        calls_diff = prev_stats.get('calls_waiting', 0) - curr_stats.get('calls_waiting', 0)
        reward += calls_diff * 2.0
        
        # Cambio en tiempo de espera (menos es mejor)
        hold_diff = prev_stats.get('hold_time', 0) - curr_stats.get('hold_time', 0)
        reward += hold_diff * 0.1
        
        # Cambio en tasa de abandono (menos es mejor)
        abandon_diff = prev_stats.get('abandon_rate', 0) - curr_stats.get('abandon_rate', 0)
        reward += abandon_diff * 0.5
        
        # Penalización por sobrecarga (muchas llamadas, pocos agentes)
        if curr_stats.get('calls_waiting', 0) > curr_stats.get('available_agents', 1) * 3:
            reward -= 5.0
        
        # Penalización por no tener llamadas cuando hay agentes (ineficiencia)
        if curr_stats.get('calls_waiting', 0) == 0 and curr_stats.get('available_agents', 0) > 2:
            reward -= 1.0
        
        # Bonus por estado óptimo (llamadas equilibradas con agentes)
        ratio = curr_stats.get('calls_waiting', 0) / max(curr_stats.get('available_agents', 1), 1)
        if 0.5 <= ratio <= 1.5:
            reward += 3.0
        
        return reward
    
    def choose_action(self, queue_stats: Dict, training: bool = True) -> Tuple[int, str]:
        """
        Elige la mejor acción usando política epsilon-greedy.
        Retorna (action_index, action_name)
        """
        state = self._get_state(queue_stats)
        q_values = self._get_q_values(state)
        
        # Exploración vs Explotación
        if training and np.random.random() < self.exploration_rate if NUMPY_AVAILABLE else False:
            action = np.random.choice(list(self.ACTIONS.keys())) if NUMPY_AVAILABLE else 2
        else:
            # Elegir la mejor acción
            action = max(q_values, key=q_values.get)
        
        self.last_state = state
        self.last_action = action
        
        return action, self.ACTIONS[action]
    
    def update(self, queue_stats: Dict, prev_stats: Dict) -> Dict:
        """
        Actualiza la tabla Q basándose en la transición de estado.
        Retorna información sobre la actualización.
        """
        if self.last_state is None or self.last_action is None:
            return {'status': 'no_previous_state'}
        
        current_state = self._get_state(queue_stats)
        reward = self._calculate_reward(prev_stats, queue_stats)
        
        # Obtener valores Q
        old_q = self.q_table[self.last_state][self.last_action]
        
        # Valor máximo Q del nuevo estado
        next_q_values = self._get_q_values(current_state)
        max_next_q = max(next_q_values.values())
        
        # Actualización Q-Learning: Q(s,a) = Q(s,a) + α[r + γ*max(Q(s',a')) - Q(s,a)]
        new_q = old_q + self.learning_rate * (reward + self.discount_factor * max_next_q - old_q)
        self.q_table[self.last_state][self.last_action] = new_q
        
        # Registrar recompensa
        self.episode_rewards.append(reward)
        self.total_episodes += 1
        
        # Decaer exploración
        self.exploration_rate = max(self.min_exploration, 
                                    self.exploration_rate * self.exploration_decay)
        
        # Guardar modelo periódicamente
        if self.total_episodes % 100 == 0:
            self._save_model()
        
        return {
            'status': 'updated',
            'state': self.last_state,
            'action': self.ACTIONS[self.last_action],
            'reward': round(reward, 2),
            'old_q': round(old_q, 3),
            'new_q': round(new_q, 3),
            'exploration_rate': round(self.exploration_rate, 3)
        }
    
    def get_dial_rate_adjustment(self, action: int) -> float:
        """
        Convierte una acción en un ajuste de dial rate.
        Retorna el nuevo factor multiplicador.
        """
        adjustments = {
            0: 0.5,   # decrease_fast: -50%
            1: 0.8,   # decrease_slow: -20%
            2: 1.0,   # maintain: sin cambio
            3: 1.2,   # increase_slow: +20%
            4: 1.5    # increase_fast: +50%
        }
        
        # Aplicar ajuste con límites
        self.current_dial_rate *= adjustments.get(action, 1.0)
        self.current_dial_rate = max(0.1, min(3.0, self.current_dial_rate))  # Límites: 10% - 300%
        
        return self.current_dial_rate
    
    def get_statistics(self) -> Dict:
        """Retorna estadísticas del aprendizaje"""
        recent_rewards = self.episode_rewards[-100:] if self.episode_rewards else [0]
        
        return {
            'total_episodes': self.total_episodes,
            'states_learned': len(self.q_table),
            'exploration_rate': round(self.exploration_rate, 3),
            'current_dial_rate': round(self.current_dial_rate, 2),
            'avg_reward_last_100': round(sum(recent_rewards) / len(recent_rewards), 2) if recent_rewards else 0,
            'max_reward_last_100': round(max(recent_rewards), 2) if recent_rewards else 0,
            'min_reward_last_100': round(min(recent_rewards), 2) if recent_rewards else 0
        }
    
    def get_recommendation(self, queue_stats: Dict) -> Dict:
        """
        Obtiene una recomendación de dial rate basada en el estado actual.
        """
        action, action_name = self.choose_action(queue_stats, training=False)
        new_rate = self.get_dial_rate_adjustment(action)
        
        # Generar explicación
        explanations = {
            'decrease_fast': '⚠️ REDUCIR MARCADO: Cola sobrecargada, reducir 50%',
            'decrease_slow': '📉 Reducir marcado: Cola con carga alta, reducir 20%',
            'maintain': '✅ Mantener: Dial rate óptimo para estado actual',
            'increase_slow': '📈 Aumentar marcado: Capacidad disponible, aumentar 20%',
            'increase_fast': '🚀 AUMENTAR MARCADO: Muchos agentes disponibles, aumentar 50%'
        }
        
        return {
            'action': action_name,
            'dial_rate_factor': round(new_rate, 2),
            'explanation': explanations.get(action_name, 'Acción desconocida'),
            'confidence': 'high' if self.total_episodes > 1000 else 'medium' if self.total_episodes > 100 else 'learning',
            'state_visits': len([1 for r in self.episode_rewards[-100:] if r > 0]) if self.episode_rewards else 0
        }


class IsolationForestAnomalyDetector:
    """
    Detector de anomalías usando Isolation Forest.
    Detecta patrones anómalos en las métricas de la cola de forma no supervisada.
    """
    
    def __init__(self, contamination: float = 0.1):
        self.contamination = contamination  # Porcentaje esperado de anomalías
        self.model: Optional[Any] = None
        self.scaler: Optional[Any] = None
        self.is_fitted = False
        self.feature_names = [
            'calls_waiting', 'hold_time', 'available_agents', 
            'busy_agents', 'paused_agents', 'abandon_rate'
        ]
        self.training_data: List[List[float]] = []
        self.min_samples_for_training = 50
        
        if SKLEARN_AVAILABLE:
            self.model = IsolationForest(
                contamination=contamination,
                n_estimators=100,
                max_samples='auto',
                random_state=42,
                n_jobs=-1
            )
            self.scaler = StandardScaler()
        else:
            logger.warning("⚠️ sklearn no disponible, Isolation Forest deshabilitado")
    
    def _extract_features(self, queue_stats: Dict) -> List[float]:
        """Extrae features de las estadísticas de cola"""
        total_members = queue_stats.get('total_members', 1)
        completed = queue_stats.get('completed', 0)
        abandoned = queue_stats.get('abandoned', 0)
        
        # Calcular tasa de abandono
        total_calls = completed + abandoned
        abandon_rate = (abandoned / total_calls * 100) if total_calls > 0 else 0
        
        return [
            queue_stats.get('calls_waiting', 0),
            queue_stats.get('hold_time', 0),
            queue_stats.get('available_members', 0),
            queue_stats.get('busy_members', 0),
            queue_stats.get('paused_members', 0),
            abandon_rate
        ]
    
    def add_sample(self, queue_stats: Dict):
        """Agrega una muestra para entrenamiento"""
        features = self._extract_features(queue_stats)
        self.training_data.append(features)
        
        # Limitar datos de entrenamiento
        if len(self.training_data) > 5000:
            self.training_data = self.training_data[-5000:]
        
        # Entrenar automáticamente cuando hay suficientes datos
        if len(self.training_data) >= self.min_samples_for_training and not self.is_fitted:
            self.fit()
        elif len(self.training_data) % 500 == 0 and self.is_fitted:
            # Re-entrenar periódicamente
            self.fit()
    
    def fit(self):
        """Entrena el modelo con los datos acumulados"""
        if not SKLEARN_AVAILABLE or len(self.training_data) < self.min_samples_for_training:
            return
        
        try:
            X = np.array(self.training_data)
            X_scaled = self.scaler.fit_transform(X)
            self.model.fit(X_scaled)
            self.is_fitted = True
            logger.info(f"🎯 Isolation Forest entrenado con {len(self.training_data)} muestras")
        except Exception as e:
            logger.error(f"Error entrenando Isolation Forest: {e}")
    
    def predict(self, queue_stats: Dict) -> Dict:
        """
        Predice si el estado actual es anómalo.
        Retorna dict con is_anomaly, score, y detalles.
        """
        if not SKLEARN_AVAILABLE:
            return {
                'is_anomaly': False,
                'score': 0.0,
                'message': 'sklearn no disponible',
                'status': 'disabled'
            }
        
        if not self.is_fitted:
            return {
                'is_anomaly': False,
                'score': 0.0,
                'message': f'Modelo entrenando... ({len(self.training_data)}/{self.min_samples_for_training} muestras)',
                'status': 'training'
            }
        
        try:
            features = self._extract_features(queue_stats)
            X = np.array([features])
            X_scaled = self.scaler.transform(X)
            
            # Predicción: 1 = normal, -1 = anomalía
            prediction = self.model.predict(X_scaled)[0]
            
            # Score de anomalía (más negativo = más anómalo)
            anomaly_score = self.model.decision_function(X_scaled)[0]
            
            is_anomaly = prediction == -1
            
            # Determinar qué feature es más anómala
            anomalous_features = []
            if is_anomaly:
                # Comparar con promedios de entrenamiento
                mean_features = np.mean(self.training_data, axis=0)
                std_features = np.std(self.training_data, axis=0)
                
                for i, (feat_val, mean_val, std_val) in enumerate(zip(features, mean_features, std_features)):
                    if std_val > 0:
                        z_score = abs(feat_val - mean_val) / std_val
                        if z_score > 2:  # Más de 2 desviaciones estándar
                            anomalous_features.append({
                                'feature': self.feature_names[i],
                                'current': round(feat_val, 1),
                                'expected': round(mean_val, 1),
                                'z_score': round(z_score, 2)
                            })
            
            # Generar mensaje explicativo
            if is_anomaly:
                if anomalous_features:
                    main_anomaly = max(anomalous_features, key=lambda x: x['z_score'])
                    message = f"🚨 ANOMALÍA: {main_anomaly['feature']} = {main_anomaly['current']} (esperado: {main_anomaly['expected']})"
                else:
                    message = "🚨 Patrón anómalo detectado (combinación inusual de métricas)"
            else:
                message = "✅ Métricas dentro de rangos normales"
            
            return {
                'is_anomaly': is_anomaly,
                'score': round(float(anomaly_score), 3),
                'message': message,
                'status': 'active',
                'anomalous_features': anomalous_features,
                'severity': 'critical' if anomaly_score < -0.3 else 'warning' if is_anomaly else 'normal'
            }
            
        except Exception as e:
            logger.error(f"Error en predicción Isolation Forest: {e}")
            return {
                'is_anomaly': False,
                'score': 0.0,
                'message': f'Error en predicción: {str(e)}',
                'status': 'error'
            }
    
    def get_statistics(self) -> Dict:
        """Retorna estadísticas del detector"""
        return {
            'is_fitted': self.is_fitted,
            'training_samples': len(self.training_data),
            'contamination': self.contamination,
            'feature_names': self.feature_names,
            'status': 'active' if self.is_fitted else 'training'
        }


class QueueAnalytics:
    """Analizador de métricas y predictor de colas usando aprendizaje"""
    
    def __init__(self, redis_client=None):
        self.redis = redis_client
        self.history: deque = deque(maxlen=ML_HISTORY_SIZE)
        self.hourly_patterns: Dict[int, List[Dict]] = {h: [] for h in range(24)}
        self.day_patterns: Dict[int, List[Dict]] = {d: [] for d in range(7)}
        self.queue_patterns: Dict[str, deque] = {}
        self.anomaly_threshold = 2.0  # Desviaciones estándar para anomalías
        self.last_training = datetime.now()
        self.predictions_cache: Dict[str, Dict] = {}
        self.optimization_history: List[Dict] = []
        
        # Nuevos componentes de ML
        self.qlearning_optimizer = QLearningDialOptimizer()
        self.isolation_forest = IsolationForestAnomalyDetector(contamination=0.1)
        self.prev_queue_stats: Dict[str, Dict] = {}  # Para calcular rewards en Q-Learning
        
        # Cargar modelo si existe
        self._load_model()
    
    def _load_model(self):
        """Carga el modelo de ML desde disco"""
        try:
            if ML_MODEL_PATH.exists():
                with open(ML_MODEL_PATH, 'rb') as f:
                    data = pickle.load(f)
                    self.history = deque(data.get('history', []), maxlen=ML_HISTORY_SIZE)
                    self.hourly_patterns = data.get('hourly_patterns', {h: [] for h in range(24)})
                    self.day_patterns = data.get('day_patterns', {d: [] for d in range(7)})
                    self.queue_patterns = data.get('queue_patterns', {})
                    logger.info(f"✅ Modelo ML cargado: {len(self.history)} muestras históricas")
        except Exception as e:
            logger.warning(f"⚠️ No se pudo cargar modelo ML: {e}")
    
    def _save_model(self):
        """Guarda el modelo de ML a disco"""
        try:
            data = {
                'history': list(self.history),
                'hourly_patterns': self.hourly_patterns,
                'day_patterns': self.day_patterns,
                'queue_patterns': {k: list(v) for k, v in self.queue_patterns.items()},
                'saved_at': datetime.now().isoformat()
            }
            with open(ML_MODEL_PATH, 'wb') as f:
                pickle.dump(data, f)
            logger.debug("💾 Modelo ML guardado")
        except Exception as e:
            logger.error(f"Error guardando modelo ML: {e}")
    
    def record_snapshot(self, queues: List[QueueStats]):
        """Registra una instantánea de las colas para análisis"""
        now = datetime.now()
        
        for queue in queues:
            snapshot = {
                'timestamp': now.isoformat(),
                'hour': now.hour,
                'day_of_week': now.weekday(),
                'minute': now.minute,
                'queue_name': queue.name,
                'calls_waiting': queue.calls,
                'hold_time': queue.holdtime,
                'talk_time': queue.talk_time,
                'total_members': len(queue.members),
                'available_members': queue.available_members,
                'busy_members': queue.busy_members,
                'paused_members': queue.paused_members,
                'completed': queue.completed,
                'abandoned': queue.abandoned,
                'service_level': queue.service_level_perf
            }
            
            # Agregar al historial general
            self.history.append(snapshot)
            
            # Agregar a patrones por hora (asegurar que existe la clave)
            if now.hour not in self.hourly_patterns:
                self.hourly_patterns[now.hour] = []
            self.hourly_patterns[now.hour].append(snapshot)
            
            # Limitar patrones por hora (últimas 1000 muestras por hora)
            if len(self.hourly_patterns[now.hour]) > 1000:
                self.hourly_patterns[now.hour] = self.hourly_patterns[now.hour][-1000:]
            
            # Agregar a patrones por día
            self.day_patterns[now.weekday()].append(snapshot)
            if len(self.day_patterns[now.weekday()]) > 1000:
                self.day_patterns[now.weekday()] = self.day_patterns[now.weekday()][-1000:]
            
            # Patrones específicos por cola
            if queue.name not in self.queue_patterns:
                self.queue_patterns[queue.name] = deque(maxlen=2000)
            self.queue_patterns[queue.name].append(snapshot)
            
            # Guardar en Redis si está disponible
            if self.redis:
                try:
                    key = f"queue_analytics:{queue.name}:history"
                    self.redis.lpush(key, json.dumps(snapshot))
                    self.redis.ltrim(key, 0, 999)  # Mantener últimas 1000
                except Exception as e:
                    logger.debug(f"Error guardando en Redis: {e}")
            
            # === NUEVOS MODELOS ML ===
            # Alimentar Isolation Forest con datos
            self.isolation_forest.add_sample(snapshot)
            
            # Actualizar Q-Learning si hay estado previo
            if queue.name in self.prev_queue_stats:
                prev_stats = self.prev_queue_stats[queue.name]
                ql_stats = {
                    'calls_waiting': queue.calls,
                    'available_agents': queue.available_members,
                    'hold_time': queue.holdtime,
                    'abandon_rate': (queue.abandoned / (queue.completed + queue.abandoned) * 100) 
                                   if (queue.completed + queue.abandoned) > 0 else 0
                }
                self.qlearning_optimizer.update(ql_stats, prev_stats)
            
            # Guardar estado actual para siguiente iteración
            self.prev_queue_stats[queue.name] = {
                'calls_waiting': queue.calls,
                'available_agents': queue.available_members,
                'hold_time': queue.holdtime,
                'abandon_rate': (queue.abandoned / (queue.completed + queue.abandoned) * 100) 
                               if (queue.completed + queue.abandoned) > 0 else 0
            }
        
        # Guardar modelo periódicamente
        if (datetime.now() - self.last_training).total_seconds() > ML_TRAINING_INTERVAL:
            self._save_model()
            self.last_training = datetime.now()
    
    def predict_queue_load(self, queue_name: str, minutes_ahead: int = 5) -> Dict:
        """Predice la carga de una cola en los próximos minutos"""
        now = datetime.now()
        target_hour = (now + timedelta(minutes=minutes_ahead)).hour
        target_day = (now + timedelta(minutes=minutes_ahead)).weekday()
        
        # Primero intentar con datos históricos por hora
        relevant_data = [
            s for s in self.hourly_patterns.get(target_hour, [])
            if s.get('queue_name') == queue_name
        ]
        
        # Si no hay suficientes datos por hora, usar los patrones de la cola
        if len(relevant_data) < 5 and queue_name in self.queue_patterns:
            relevant_data = list(self.queue_patterns[queue_name])
            logger.debug(f"Usando {len(relevant_data)} muestras de queue_patterns para {queue_name}")
        
        # Si aún no hay datos, usar el historial general filtrado
        if len(relevant_data) < 5:
            relevant_data = [s for s in self.history if s.get('queue_name') == queue_name]
            logger.debug(f"Usando {len(relevant_data)} muestras de history para {queue_name}")
        
        # Si hay al menos 1 muestra, hacer predicción básica
        if len(relevant_data) >= 1:
            # Calcular promedios y tendencias
            calls_waiting = [s['calls_waiting'] for s in relevant_data[-100:]]
            hold_times = [s['hold_time'] for s in relevant_data[-100:]]
            available = [s['available_members'] for s in relevant_data[-100:]]
            completed = [s.get('completed', 0) for s in relevant_data[-100:]]
            abandoned = [s.get('abandoned', 0) for s in relevant_data[-100:]]
            
            avg_calls = statistics.mean(calls_waiting) if calls_waiting else 0
            avg_hold = statistics.mean(hold_times) if hold_times else 0
            avg_available = statistics.mean(available) if available else 0
            
            # Calcular tendencia reciente
            if len(calls_waiting) >= 10:
                recent_trend = statistics.mean(calls_waiting[-5:]) - statistics.mean(calls_waiting[-10:-5])
            elif len(calls_waiting) >= 3:
                recent_trend = calls_waiting[-1] - statistics.mean(calls_waiting[:-1])
            else:
                recent_trend = 0
            
            # Calcular tendencia de agentes
            if len(available) >= 3:
                agent_trend = available[-1] - statistics.mean(available[:-1])
            else:
                agent_trend = 0
            
            predicted_calls = max(0, avg_calls + (recent_trend * (minutes_ahead / 5)))
            predicted_agents = max(0, avg_available + agent_trend)
            
            # Calcular métricas adicionales
            total_completed = sum(completed) if completed else 0
            total_abandoned = sum(abandoned) if abandoned else 0
            abandon_rate = (total_abandoned / (total_completed + total_abandoned) * 100) if (total_completed + total_abandoned) > 0 else 0
            
            # Determinar nivel de alerta basado en múltiples factores
            if predicted_calls > 5 and avg_available < 2:
                alert_level = "high"
                recommendation = f"⚠️ CRÍTICO: {int(predicted_calls)} llamadas esperadas con solo {avg_available:.0f} agentes. Agregar agentes urgentemente."
            elif predicted_calls > avg_calls * 1.5 or (avg_calls > 0 and avg_available == 0):
                alert_level = "high"
                recommendation = f"Considere agregar más agentes. Tendencia: {'+' if recent_trend > 0 else ''}{recent_trend:.1f} llamadas"
            elif predicted_calls > avg_calls * 1.2 or abandon_rate > 10:
                alert_level = "medium"
                recommendation = f"Monitoree la cola. Tasa de abandono: {abandon_rate:.1f}%"
            else:
                alert_level = "normal"
                recommendation = f"Carga estable. Promedio: {avg_calls:.1f} llamadas, {avg_hold:.0f}s espera"
            
            # Determinar confianza basada en cantidad de datos
            if len(relevant_data) >= 50:
                confidence = 'high'
            elif len(relevant_data) >= 20:
                confidence = 'medium'
            elif len(relevant_data) >= 5:
                confidence = 'low'
            else:
                confidence = 'very_low'
            
            return {
                'queue': queue_name,
                'minutes_ahead': minutes_ahead,
                'predicted_calls_waiting': round(predicted_calls, 1),
                'predicted_hold_time': round(avg_hold, 0),
                'predicted_available_agents': round(predicted_agents, 1),
                'current_calls': calls_waiting[-1] if calls_waiting else 0,
                'current_agents': available[-1] if available else 0,
                'trend': 'increasing' if recent_trend > 0.5 else 'decreasing' if recent_trend < -0.5 else 'stable',
                'agent_trend': 'increasing' if agent_trend > 0.5 else 'decreasing' if agent_trend < -0.5 else 'stable',
                'alert_level': alert_level,
                'recommendation': recommendation,
                'confidence': confidence,
                'based_on_samples': len(relevant_data),
                'abandon_rate': round(abandon_rate, 1),
                'avg_hold_time': round(avg_hold, 0),
                'total_completed': total_completed,
                'total_abandoned': total_abandoned
            }
        
        # Sin datos - devolver predicción basada en valores actuales si existen
        return {
            'queue': queue_name,
            'minutes_ahead': minutes_ahead,
            'predicted_calls_waiting': 0,
            'predicted_hold_time': 0,
            'predicted_available_agents': 0,
            'current_calls': 0,
            'current_agents': 0,
            'trend': 'unknown',
            'agent_trend': 'unknown',
            'alert_level': 'unknown',
            'recommendation': 'Recopilando datos... La predicción mejorará con más muestras.',
            'confidence': 'none',
            'based_on_samples': 0,
            'message': f'Iniciando recopilación de datos para {queue_name}. Espere unos minutos.'
        }
    
    def detect_anomalies(self, queues: List[QueueStats]) -> List[Dict]:
        """Detecta anomalías en las métricas de las colas"""
        anomalies = []
        
        for queue in queues:
            if queue.name not in self.queue_patterns:
                continue
            
            history = list(self.queue_patterns[queue.name])
            if len(history) < 20:
                continue
            
            # Obtener estadísticas históricas
            hist_calls = [s['calls_waiting'] for s in history[-100:]]
            hist_hold = [s['hold_time'] for s in history[-100:]]
            hist_abandoned = [s['abandoned'] for s in history[-100:]]
            
            avg_calls = statistics.mean(hist_calls)
            std_calls = statistics.stdev(hist_calls) if len(hist_calls) > 1 else 0
            
            avg_hold = statistics.mean(hist_hold)
            std_hold = statistics.stdev(hist_hold) if len(hist_hold) > 1 else 0
            
            # Detectar anomalías en llamadas en espera
            if std_calls > 0 and queue.calls > avg_calls + (self.anomaly_threshold * std_calls):
                anomalies.append({
                    'queue': queue.name,
                    'type': 'high_calls_waiting',
                    'severity': 'warning' if queue.calls < avg_calls * 2 else 'critical',
                    'current_value': queue.calls,
                    'expected_value': round(avg_calls, 1),
                    'message': f"Llamadas en espera ({queue.calls}) significativamente mayor al promedio ({avg_calls:.1f})"
                })
            
            # Detectar anomalías en tiempo de espera
            if std_hold > 0 and queue.holdtime > avg_hold + (self.anomaly_threshold * std_hold):
                anomalies.append({
                    'queue': queue.name,
                    'type': 'high_hold_time',
                    'severity': 'warning' if queue.holdtime < avg_hold * 2 else 'critical',
                    'current_value': queue.holdtime,
                    'expected_value': round(avg_hold, 1),
                    'message': f"Tiempo de espera ({queue.holdtime}s) significativamente mayor al promedio ({avg_hold:.1f}s)"
                })
            
            # Detectar falta de agentes disponibles
            if queue.available_members == 0 and queue.calls > 0:
                anomalies.append({
                    'queue': queue.name,
                    'type': 'no_available_agents',
                    'severity': 'critical',
                    'current_value': queue.available_members,
                    'message': f"Sin agentes disponibles con {queue.calls} llamadas en espera"
                })
            
            # Detectar alta tasa de abandono
            if len(hist_abandoned) > 1:
                recent_abandoned = queue.abandoned - hist_abandoned[-1] if len(hist_abandoned) > 0 else 0
                if recent_abandoned > 5:  # Más de 5 abandonos desde última lectura
                    anomalies.append({
                        'queue': queue.name,
                        'type': 'high_abandon_rate',
                        'severity': 'warning',
                        'current_value': recent_abandoned,
                        'message': f"Alta tasa de abandono: {recent_abandoned} en el último intervalo"
                    })
        
        return anomalies
    
    def get_optimization_suggestions(self, queues: List[QueueStats]) -> List[Dict]:
        """Genera sugerencias de optimización basadas en patrones"""
        suggestions = []
        now = datetime.now()
        
        for queue in queues:
            # Análisis de eficiencia de agentes
            if queue.available_members > queue.calls * 2 and queue.calls > 0:
                suggestions.append({
                    'queue': queue.name,
                    'type': 'overstaffed',
                    'priority': 'low',
                    'message': f"Cola posiblemente sobredimensionada: {queue.available_members} agentes disponibles para {queue.calls} llamadas",
                    'action': 'Considere reasignar agentes a otras colas'
                })
            
            if queue.calls > queue.available_members * 3 and queue.available_members > 0:
                suggestions.append({
                    'queue': queue.name,
                    'type': 'understaffed',
                    'priority': 'high',
                    'message': f"Cola subdimensionada: {queue.calls} llamadas para {queue.available_members} agentes",
                    'action': 'Agregar más agentes o activar agentes en pausa'
                })
            
            # Análisis de pausas
            if queue.paused_members > queue.available_members and queue.calls > 2:
                suggestions.append({
                    'queue': queue.name,
                    'type': 'too_many_paused',
                    'priority': 'medium',
                    'message': f"Muchos agentes en pausa ({queue.paused_members}) con llamadas esperando ({queue.calls})",
                    'action': 'Solicitar a agentes que regresen de pausa'
                })
            
            # Análisis de tiempo de espera
            if queue.holdtime > 120 and queue.calls > 0:  # Más de 2 minutos
                suggestions.append({
                    'queue': queue.name,
                    'type': 'high_wait_time',
                    'priority': 'high',
                    'message': f"Tiempo de espera alto: {queue.holdtime}s promedio",
                    'action': 'Revisar capacidad de agentes y flujo de llamadas'
                })
            
            # Predicción y sugerencia proactiva
            prediction = self.predict_queue_load(queue.name, minutes_ahead=15)
            if prediction.get('alert_level') == 'high':
                suggestions.append({
                    'queue': queue.name,
                    'type': 'predicted_overload',
                    'priority': 'medium',
                    'message': f"Se predice aumento de carga en los próximos 15 minutos",
                    'action': prediction.get('recommendation', 'Preparar recursos adicionales')
                })
        
        return suggestions
    
    def get_analytics_summary(self, queues: List[QueueStats]) -> Dict:
        """Genera un resumen analítico completo"""
        # Obtener recomendaciones de Q-Learning para cada cola
        qlearning_recommendations = {}
        isolation_forest_results = {}
        
        for queue in queues:
            # Preparar stats para ML
            total_calls = queue.completed + queue.abandoned
            queue_ml_stats = {
                'calls_waiting': queue.calls,
                'available_agents': queue.available_members,
                'hold_time': queue.holdtime,
                'abandon_rate': (queue.abandoned / total_calls * 100) if total_calls > 0 else 0,
                'total_members': len(queue.members),
                'available_members': queue.available_members,
                'busy_members': queue.busy_members,
                'paused_members': queue.paused_members,
                'completed': queue.completed,
                'abandoned': queue.abandoned
            }
            
            # Q-Learning: Recomendación de dial rate
            qlearning_recommendations[queue.name] = self.qlearning_optimizer.get_recommendation(queue_ml_stats)
            
            # Isolation Forest: Detección de anomalías
            isolation_forest_results[queue.name] = self.isolation_forest.predict(queue_ml_stats)
        
        return {
            'timestamp': datetime.now().isoformat(),
            'total_samples': len(self.history),
            'predictions': {q.name: self.predict_queue_load(q.name) for q in queues},
            'anomalies': self.detect_anomalies(queues),
            'suggestions': self.get_optimization_suggestions(queues),
            'patterns': {
                'current_hour_avg_load': self._get_hour_avg(datetime.now().hour),
                'current_day_avg_load': self._get_day_avg(datetime.now().weekday())
            },
            # Nuevos campos de ML avanzado
            'ml_dial_optimization': qlearning_recommendations,
            'ml_anomaly_detection': isolation_forest_results,
            'ml_statistics': {
                'qlearning': self.qlearning_optimizer.get_statistics(),
                'isolation_forest': self.isolation_forest.get_statistics()
            }
        }
    
    def get_dial_rate_recommendation(self, queue_name: str, queue_stats: Dict = None) -> Dict:
        """
        Obtiene la recomendación de dial rate del Q-Learning para una cola específica.
        Útil para el discador automático.
        """
        if queue_stats is None:
            # Usar último estado conocido
            queue_stats = self.prev_queue_stats.get(queue_name, {
                'calls_waiting': 0,
                'available_agents': 1,
                'hold_time': 0,
                'abandon_rate': 0
            })
        
        return self.qlearning_optimizer.get_recommendation(queue_stats)
    
    def check_anomaly(self, queue_stats: Dict) -> Dict:
        """
        Verifica si el estado actual de la cola es anómalo usando Isolation Forest.
        """
        return self.isolation_forest.predict(queue_stats)
    
    def get_ml_statistics(self) -> Dict:
        """Retorna estadísticas de todos los modelos de ML"""
        return {
            'qlearning': self.qlearning_optimizer.get_statistics(),
            'isolation_forest': self.isolation_forest.get_statistics(),
            'history_samples': len(self.history),
            'queues_tracked': list(self.queue_patterns.keys())
        }
    
    def _get_hour_avg(self, hour: int) -> Dict:
        """Obtiene promedios para una hora específica"""
        data = self.hourly_patterns.get(hour, [])
        if not data:
            return {'calls': 0, 'hold_time': 0, 'samples': 0}
        
        return {
            'calls': round(statistics.mean([s['calls_waiting'] for s in data[-100:]]), 1),
            'hold_time': round(statistics.mean([s['hold_time'] for s in data[-100:]]), 1),
            'samples': len(data)
        }
    
    def _get_day_avg(self, day: int) -> Dict:
        """Obtiene promedios para un día específico"""
        data = self.day_patterns.get(day, [])
        if not data:
            return {'calls': 0, 'hold_time': 0, 'samples': 0}
        
        return {
            'calls': round(statistics.mean([s['calls_waiting'] for s in data[-100:]]), 1),
            'hold_time': round(statistics.mean([s['hold_time'] for s in data[-100:]]), 1),
            'samples': len(data)
        }


class AsteriskAMI:
    """Cliente AMI para Asterisk"""
    
    def __init__(self, host: str = AMI_HOST, port: int = AMI_PORT, 
                 username: str = AMI_USERNAME, secret: str = AMI_SECRET):
        self.host = host
        self.port = port
        self.username = username
        self.secret = secret
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.connected = False
        self.action_id = 0
        self._lock = asyncio.Lock()
        
    async def connect(self) -> bool:
        """Conecta al AMI de Asterisk"""
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=10
            )
            
            # Leer banner de bienvenida
            banner = await self.reader.readline()
            logger.debug(f"AMI Banner: {banner.decode().strip()}")
            
            # Login
            response = await self._send_action({
                "Action": "Login",
                "Username": self.username,
                "Secret": self.secret
            })
            
            if response.get("Response") == "Success":
                self.connected = True
                logger.info(f"✅ Conectado a Asterisk AMI en {self.host}:{self.port}")
                return True
            else:
                logger.error(f"❌ Error de autenticación: {response.get('Message', 'Unknown')}")
                return False
                
        except asyncio.TimeoutError:
            logger.error(f"❌ Timeout conectando a {self.host}:{self.port}")
            return False
        except ConnectionRefusedError:
            logger.error(f"❌ Conexión rechazada a {self.host}:{self.port}")
            return False
        except Exception as e:
            logger.error(f"❌ Error conectando a AMI: {e}")
            return False
    
    async def disconnect(self):
        """Desconecta del AMI"""
        if self.writer:
            try:
                await self._send_action({"Action": "Logoff"})
            except:
                pass
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except:
                pass
        self.connected = False
        logger.info("🔌 Desconectado de AMI")
    
    async def _send_action(self, action: Dict) -> Dict:
        """Envía una acción al AMI y retorna la respuesta"""
        async with self._lock:
            self.action_id += 1
            action["ActionID"] = str(self.action_id)
            
            # Construir mensaje
            message = "\r\n".join(f"{k}: {v}" for k, v in action.items()) + "\r\n\r\n"
            
            self.writer.write(message.encode())
            await self.writer.drain()
            
            # Leer respuesta
            response = {}
            while True:
                line = await self.reader.readline()
                line = line.decode().strip()
                
                if not line:  # Línea vacía = fin del mensaje
                    break
                    
                if ": " in line:
                    key, value = line.split(": ", 1)
                    response[key] = value
            
            return response
    
    async def _read_until_event_complete(self, end_event: str) -> List[Dict]:
        """Lee eventos hasta encontrar el evento de finalización"""
        events = []
        current_event = {}
        
        while True:
            try:
                line = await asyncio.wait_for(self.reader.readline(), timeout=10)
                line = line.decode().strip()
                
                if not line:  # Línea vacía = fin del evento
                    if current_event:
                        events.append(current_event)
                        
                        # Verificar si es el evento de finalización
                        if current_event.get("Event") == end_event:
                            break
                        
                        current_event = {}
                    continue
                
                if ": " in line:
                    key, value = line.split(": ", 1)
                    current_event[key] = value
                    
            except asyncio.TimeoutError:
                logger.warning("⚠️ Timeout esperando eventos")
                break
                
        return events
    
    async def get_queues(self) -> List[QueueStats]:
        """Obtiene el estado de todas las colas"""
        if not self.connected:
            logger.error("No conectado a AMI")
            return []
        
        try:
            # Enviar QueueStatus
            await self._send_action({"Action": "QueueStatus"})
            
            # Leer todos los eventos hasta QueueStatusComplete
            events = await self._read_until_event_complete("QueueStatusComplete")
            
            queues: Dict[str, QueueStats] = {}
            
            for event in events:
                event_type = event.get("Event", "")
                
                if event_type == "QueueParams":
                    queue_name = event.get("Queue", "")
                    queues[queue_name] = QueueStats(
                        name=queue_name,
                        max_members=int(event.get("Max", 0)),
                        strategy=event.get("Strategy", "ringall"),
                        calls=int(event.get("Calls", 0)),
                        holdtime=int(event.get("Holdtime", 0)),
                        talk_time=int(event.get("TalkTime", 0)),
                        completed=int(event.get("Completed", 0)),
                        abandoned=int(event.get("Abandoned", 0)),
                        service_level=int(event.get("ServiceLevel", 0)),
                        service_level_perf=float(event.get("ServiceLevelPerf", 0)),
                        service_level_perf2=float(event.get("ServiceLevelPerf2", 0)),
                        weight=int(event.get("Weight", 0))
                    )
                    
                elif event_type == "QueueMember":
                    queue_name = event.get("Queue", "")
                    if queue_name in queues:
                        member = QueueMember(
                            name=event.get("Name", ""),
                            interface=event.get("StateInterface", event.get("Location", "")),
                            state_interface=event.get("StateInterface", ""),
                            membership=event.get("Membership", "dynamic"),
                            penalty=int(event.get("Penalty", 0)),
                            calls_taken=int(event.get("CallsTaken", 0)),
                            last_call=int(event.get("LastCall", 0)),
                            last_pause=int(event.get("LastPause", 0)),
                            in_call=event.get("InCall", "0") == "1",
                            status=int(event.get("Status", 0)),
                            paused=event.get("Paused", "0") == "1",
                            paused_reason=event.get("PausedReason", ""),
                            ring_in_use=event.get("Ringinuse", "1") == "1"
                        )
                        queues[queue_name].members.append(member)
                        
                elif event_type == "QueueEntry":
                    queue_name = event.get("Queue", "")
                    if queue_name in queues:
                        caller = QueueCall(
                            position=int(event.get("Position", 0)),
                            channel=event.get("Channel", ""),
                            uniqueid=event.get("Uniqueid", ""),
                            caller_id_num=event.get("CallerIDNum", ""),
                            caller_id_name=event.get("CallerIDName", ""),
                            connected_line_num=event.get("ConnectedLineNum", ""),
                            connected_line_name=event.get("ConnectedLineName", ""),
                            wait=int(event.get("Wait", 0)),
                            priority=int(event.get("Priority", 0))
                        )
                        queues[queue_name].callers.append(caller)
            
            return list(queues.values())
        except Exception as e:
            logger.error(f"Error obteniendo colas: {e}")
            return []
    
    async def get_queue_summary(self) -> List[Dict]:
        """Obtiene un resumen rápido de las colas"""
        if not self.connected:
            return []
        
        await self._send_action({"Action": "QueueSummary"})
        events = await self._read_until_event_complete("QueueSummaryComplete")
        
        summaries = []
        for event in events:
            if event.get("Event") == "QueueSummary":
                summaries.append({
                    "queue": event.get("Queue", ""),
                    "logged_in": int(event.get("LoggedIn", 0)),
                    "available": int(event.get("Available", 0)),
                    "callers": int(event.get("Callers", 0)),
                    "hold_time": int(event.get("HoldTime", 0)),
                    "talk_time": int(event.get("TalkTime", 0)),
                    "longest_hold_time": int(event.get("LongestHoldTime", 0))
                })
        
        return summaries


class QueueWebSocketServer:
    """Servidor WebSocket para transmitir estado de colas en tiempo real"""
    
    def __init__(self, host: str = WS_HOST, port: int = WS_PORT,
                 ssl_cert: str = None, ssl_key: str = None):
        self.host = host
        self.port = port
        self.ssl_cert = ssl_cert
        self.ssl_key = ssl_key
        self.clients: Set[websockets.WebSocketServerProtocol] = set()
        self.ami: Optional[AsteriskAMI] = None
        self.analytics: Optional[QueueAnalytics] = None
        self.redis_client = None
        self.running = False
        self.last_data: Dict = {}
        self.last_hash: str = ""
        
        # Inicializar Redis si está disponible
        if REDIS_AVAILABLE:
            try:
                self.redis_client = redis.Redis(
                    host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
                    decode_responses=True
                )
                self.redis_client.ping()
                logger.info("✅ Conectado a Redis para persistencia")
            except Exception as e:
                logger.warning(f"⚠️ Redis no disponible: {e}")
                self.redis_client = None
        
        # Inicializar analytics
        self.analytics = QueueAnalytics(self.redis_client)
    
    def _create_ssl_context(self) -> Optional[ssl.SSLContext]:
        """Crea contexto SSL si hay certificados configurados"""
        if self.ssl_cert and self.ssl_key:
            try:
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                ssl_context.load_cert_chain(self.ssl_cert, self.ssl_key)
                return ssl_context
            except Exception as e:
                logger.error(f"Error cargando certificados SSL: {e}")
        return None
    
    async def register(self, websocket: websockets.WebSocketServerProtocol):
        """Registra un nuevo cliente WebSocket"""
        self.clients.add(websocket)
        client_info = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
        logger.info(f"📱 Cliente conectado: {client_info} (Total: {len(self.clients)})")
        
        # Enviar datos actuales inmediatamente
        if self.last_data:
            try:
                await websocket.send(json.dumps({
                    "type": "initial_data",
                    "data": self.last_data
                }))
            except:
                pass
    
    async def unregister(self, websocket: websockets.WebSocketServerProtocol):
        """Desregistra un cliente WebSocket"""
        self.clients.discard(websocket)
        logger.info(f"📴 Cliente desconectado (Total: {len(self.clients)})")
    
    async def broadcast(self, message: Dict):
        """Envía mensaje a todos los clientes conectados"""
        if not self.clients:
            return
        
        message_str = json.dumps(message)
        
        # Enviar a todos los clientes
        disconnected = set()
        for client in self.clients:
            try:
                await client.send(message_str)
            except websockets.exceptions.ConnectionClosed:
                disconnected.add(client)
            except Exception as e:
                logger.debug(f"Error enviando a cliente: {e}")
                disconnected.add(client)
        
        # Limpiar clientes desconectados
        for client in disconnected:
            self.clients.discard(client)
    
    async def handle_client(self, websocket: websockets.WebSocketServerProtocol, path: str):
        """Maneja la conexión de un cliente"""
        await self.register(websocket)
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    await self.handle_message(websocket, data)
                except json.JSONDecodeError:
                    await websocket.send(json.dumps({
                        "type": "error",
                        "message": "Invalid JSON"
                    }))
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            await self.unregister(websocket)
    
    async def handle_message(self, websocket: websockets.WebSocketServerProtocol, data: Dict):
        """Procesa mensajes recibidos de clientes"""
        msg_type = data.get("type", "")
        
        if msg_type == "get_queues":
            # Solicitud de estado actual de colas
            await websocket.send(json.dumps({
                "type": "queues",
                "data": self.last_data
            }))
        
        elif msg_type == "get_analytics":
            # Solicitud de análisis
            if self.analytics and self.last_data.get("queues"):
                queues = [QueueStats(**q) if isinstance(q, dict) else q 
                          for q in self.last_data.get("queues", [])]
                analytics = self.analytics.get_analytics_summary(queues)
                await websocket.send(json.dumps({
                    "type": "analytics",
                    "data": analytics
                }))
        
        elif msg_type == "get_prediction":
            # Solicitud de predicción para una cola específica
            queue_name = data.get("queue_name")
            minutes = data.get("minutes_ahead", 5)
            if queue_name and self.analytics:
                prediction = self.analytics.predict_queue_load(queue_name, minutes)
                await websocket.send(json.dumps({
                    "type": "prediction",
                    "data": prediction
                }))
        
        elif msg_type == "subscribe_queue":
            # Suscribirse a actualizaciones de una cola específica
            # Por ahora todos reciben todas las actualizaciones
            pass
        
        elif msg_type == "dialer_status":
            # Endpoint para el discador - obtener estado de cola para decisiones de marcado
            queue_name = data.get("queue_name")
            if queue_name:
                dialer_info = self._get_dialer_status(queue_name)
                await websocket.send(json.dumps({
                    "type": "dialer_status",
                    "data": dialer_info
                }))
            else:
                await websocket.send(json.dumps({
                    "type": "error",
                    "message": "queue_name es requerido para dialer_status"
                }))
        
        elif msg_type == "dialer_config":
            # Endpoint para configurar parámetros del discador
            queue_name = data.get("queue_name")
            config = data.get("config", {})
            if queue_name:
                # Guardar configuración del discador para esta cola
                if not hasattr(self, 'dialer_configs'):
                    self.dialer_configs = {}
                self.dialer_configs[queue_name] = {
                    "max_ratio": config.get("max_ratio", 3),  # Máximo ratio llamadas/agente
                    "min_agents": config.get("min_agents", 1),  # Mínimo agentes para marcar
                    "max_wait_calls": config.get("max_wait_calls", 5),  # Máx llamadas en espera
                    "pause_on_abandon_rate": config.get("pause_on_abandon_rate", 15),  # % abandono para pausar
                    # Parámetros de SobreDiscado
                    "overdial_after_seconds": config.get("overdial_after_seconds", 0),  # Sobrediscar después de X seg libres
                    "overdial_percent": config.get("overdial_percent", 0),  # % de sobrediscado (multiplicador)
                    "overdial_multiplier": config.get("overdial_multiplier", 1),  # Multiplicador base (x1, x2, etc)
                    "max_channels": config.get("max_channels", 0),  # Canales máximos (0 = sin límite)
                    "updated_at": datetime.now().isoformat()
                }
                await websocket.send(json.dumps({
                    "type": "dialer_config_updated",
                    "data": {
                        "queue_name": queue_name,
                        "config": self.dialer_configs[queue_name]
                    }
                }))
        
        elif msg_type == "ping":
            await websocket.send(json.dumps({"type": "pong"}))
    
    def _get_dialer_status(self, queue_name: str) -> Dict:
        """
        Obtiene el estado de una cola optimizado para el discador.
        Incluye métricas y recomendaciones para decisiones de marcado.
        """
        # Buscar la cola en los datos actuales
        queues = self.last_data.get("queues", [])
        queue_data = None
        
        for q in queues:
            q_name = q.get("name") if isinstance(q, dict) else q.name
            if q_name == queue_name:
                queue_data = q if isinstance(q, dict) else q.to_dict()
                break
        
        if not queue_data:
            return {
                "queue_name": queue_name,
                "found": False,
                "error": f"Cola '{queue_name}' no encontrada",
                "can_dial": False,
                "timestamp": datetime.now().isoformat()
            }
        
        # Extraer métricas relevantes
        members = queue_data.get("members", {})
        available_agents = members.get("available", 0)
        busy_agents = members.get("busy", 0)
        paused_agents = members.get("paused", 0)
        total_agents = members.get("total", 0)
        
        calls_waiting = queue_data.get("calls_waiting", 0)
        hold_time = queue_data.get("hold_time", 0)
        completed = queue_data.get("completed", 0)
        abandoned = queue_data.get("abandoned", 0)
        service_level = queue_data.get("service_level", 0)
        
        # Calcular tasa de abandono
        total_calls = completed + abandoned
        abandon_rate = (abandoned / total_calls * 100) if total_calls > 0 else 0
        
        # Obtener configuración del discador (si existe)
        dialer_config = getattr(self, 'dialer_configs', {}).get(queue_name, {
            "max_ratio": 3,
            "min_agents": 1,
            "max_wait_calls": 5,
            "pause_on_abandon_rate": 15,
            "overdial_after_seconds": 0,
            "overdial_percent": 0,
            "overdial_multiplier": 1,
            "max_channels": 0
        })
        
        # Calcular capacidad de marcado
        # Lógica: cuántas llamadas puede manejar basándose en agentes disponibles
        
        # Obtener detalles de agentes (necesario para cálculos de sobrediscado)
        agent_details = members.get("details", [])
        
        # Obtener parámetros de sobrediscado
        overdial_after_sec = dialer_config.get("overdial_after_seconds", 0)
        overdial_percent = dialer_config.get("overdial_percent", 0)
        overdial_multiplier = dialer_config.get("overdial_multiplier", 1)
        max_channels = dialer_config.get("max_channels", 0)
        
        # Calcular agentes elegibles para sobrediscado (tiempo libre > overdial_after_sec)
        overdial_eligible_agents = 0
        current_time = int(datetime.now().timestamp())
        for agent in agent_details:
            if not agent.get("paused", False) and agent.get("status") == 1:  # No en uso y no pausado
                last_call_time = agent.get("last_call", 0) if isinstance(agent.get("last_call"), int) else 0
                if last_call_time > 0:
                    idle_time = current_time - last_call_time
                    if idle_time >= overdial_after_sec:
                        overdial_eligible_agents += 1
                elif overdial_after_sec == 0:
                    overdial_eligible_agents += 1
        
        if available_agents > 0:
            # Capacidad base: agentes disponibles * ratio
            dial_capacity = available_agents * dialer_config["max_ratio"]
            
            # Aplicar sobrediscado si está configurado y hay agentes elegibles
            overdial_extra = 0
            if overdial_after_sec >= 0 and overdial_percent > 0 and overdial_eligible_agents > 0:
                # Calcular llamadas extra por sobrediscado
                overdial_extra = int(overdial_eligible_agents * (overdial_percent / 100) * overdial_multiplier)
                dial_capacity += overdial_extra
            
            # Restar llamadas ya en espera
            dial_capacity = max(0, dial_capacity - calls_waiting)
            
            # Aplicar límite de canales máximos si está configurado
            if max_channels > 0:
                # Calcular llamadas activas actuales (en cola + atendidas)
                current_active_calls = calls_waiting + busy_agents
                available_channels = max(0, max_channels - current_active_calls)
                dial_capacity = min(dial_capacity, available_channels)
        else:
            dial_capacity = 0
            overdial_extra = 0
            overdial_eligible_agents = 0
        
        # Determinar si puede marcar y por qué
        can_dial = True
        dial_reason = "OK - Capacidad disponible"
        dial_recommendation = "proceed"  # proceed, slow, pause, stop, accelerate
        
        # STOP: No hay agentes disponibles - no marcar
        if available_agents < dialer_config["min_agents"]:
            can_dial = False
            dial_reason = f"Sin agentes disponibles ({available_agents} < {dialer_config['min_agents']})"
            dial_recommendation = "stop"
        
        # PAUSE: Demasiadas llamadas en espera Y pocos agentes - sistema saturado
        elif calls_waiting >= dialer_config["max_wait_calls"] and available_agents == 0:
            can_dial = False
            dial_reason = f"Sistema saturado: {calls_waiting} llamadas en espera sin agentes disponibles"
            dial_recommendation = "pause"
        
        # SLOW: Muchas llamadas en espera - reducir velocidad
        elif calls_waiting >= dialer_config["max_wait_calls"]:
            can_dial = True
            dial_reason = f"Muchas llamadas en espera ({calls_waiting}) - reducir velocidad"
            dial_recommendation = "slow"
        
        # SLOW: Tasa de abandono alta - reducir velocidad
        elif abandon_rate >= dialer_config["pause_on_abandon_rate"]:
            can_dial = True
            dial_reason = f"Tasa de abandono alta ({abandon_rate:.1f}%) - reducir velocidad"
            dial_recommendation = "slow"
        
        # SLOW: Solo 1 agente con llamadas en espera
        elif available_agents == 1 and calls_waiting >= 2:
            can_dial = True
            dial_reason = f"1 agente con {calls_waiting} llamadas en espera - reducir velocidad"
            dial_recommendation = "slow"
        
        # PROCEED: Condiciones normales
        else:
            can_dial = True
            dial_reason = f"OK - {available_agents} agentes disponibles, {calls_waiting} en espera"
            dial_recommendation = "proceed"
        
        # Calcular velocidad sugerida (llamadas por minuto)
        if dial_recommendation == "stop":
            suggested_rate = 0
        elif dial_recommendation == "pause":
            suggested_rate = 0
        elif dial_recommendation == "slow":
            suggested_rate = max(1, available_agents)
        else:  # proceed
            suggested_rate = available_agents * dialer_config["max_ratio"]
        
        # Obtener predicción si está disponible
        prediction = None
        if self.analytics:
            pred = self.analytics.predict_queue_load(queue_name, 5)
            if pred.get("confidence") != "none":
                prediction = {
                    "calls_in_5min": pred.get("predicted_calls_waiting", 0),
                    "agents_in_5min": pred.get("predicted_available_agents", 0),
                    "trend": pred.get("trend", "unknown"),
                    "alert_level": pred.get("alert_level", "normal")
                }
        
        # Contar agentes por cada estado
        # agent_details ya definido arriba
        status_counts = {
            "unknown": 0,        # 0 - Desconocido
            "not_in_use": 0,     # 1 - No en uso (disponible)
            "in_use": 0,         # 2 - En uso
            "busy": 0,           # 3 - Ocupado
            "invalid": 0,        # 4 - Inválido
            "unavailable": 0,    # 5 - No disponible
            "ringing": 0,        # 6 - Timbrando
            "ringing_in_use": 0, # 7 - Timbrando en uso
            "on_hold": 0         # 8 - En espera
        }
        
        # Contadores de pausados por estado
        paused_counts = {
            "unknown": 0,
            "not_in_use": 0,
            "in_use": 0,
            "busy": 0,
            "invalid": 0,
            "unavailable": 0,
            "ringing": 0,
            "ringing_in_use": 0,
            "on_hold": 0
        }
        
        status_map = {
            0: "unknown",
            1: "not_in_use",
            2: "in_use",
            3: "busy",
            4: "invalid",
            5: "unavailable",
            6: "ringing",
            7: "ringing_in_use",
            8: "on_hold"
        }
        
        for agent in agent_details:
            agent_status = agent.get("status", 0)
            status_key = status_map.get(agent_status, "unknown")
            status_counts[status_key] += 1
            if agent.get("paused", False):
                paused_counts[status_key] += 1
        
        return {
            "queue_name": queue_name,
            "found": True,
            "timestamp": datetime.now().isoformat(),
            
            # Estado actual de agentes - resumen
            "agents": {
                "available": available_agents,
                "busy": busy_agents,
                "paused": paused_agents,
                "total": total_agents
            },
            
            # Desglose detallado por estado
            "agents_by_status": {
                "unknown": status_counts["unknown"],           # 0 - Desconocido
                "not_in_use": status_counts["not_in_use"],     # 1 - No en uso (disponible)
                "in_use": status_counts["in_use"],             # 2 - En uso (en llamada)
                "busy": status_counts["busy"],                 # 3 - Ocupado
                "invalid": status_counts["invalid"],           # 4 - Inválido
                "unavailable": status_counts["unavailable"],   # 5 - No disponible (desconectado)
                "ringing": status_counts["ringing"],           # 6 - Timbrando
                "ringing_in_use": status_counts["ringing_in_use"], # 7 - Timbrando en uso
                "on_hold": status_counts["on_hold"]            # 8 - En espera
            },
            
            # Pausados por estado (para ver quienes están pausados en cada estado)
            "paused_by_status": paused_counts,
            
            # Estado de llamadas
            "calls": {
                "waiting": calls_waiting,
                "hold_time_avg": hold_time,
                "completed": completed,
                "abandoned": abandoned,
                "abandon_rate": round(abandon_rate, 1),
                "service_level": service_level
            },
            
            # Decisiones para el discador
            "dialer": {
                "can_dial": can_dial,
                "reason": dial_reason,
                "recommendation": dial_recommendation,  # proceed, slow, pause, stop, accelerate
                "dial_capacity": dial_capacity,
                "suggested_rate_per_min": suggested_rate
            },
            
            # Información de SobreDiscado
            "overdial": {
                "enabled": overdial_percent > 0 and overdial_after_sec >= 0,
                "after_seconds": overdial_after_sec,
                "percent": overdial_percent,
                "multiplier": overdial_multiplier,
                "eligible_agents": overdial_eligible_agents,
                "extra_calls": overdial_extra if 'overdial_extra' in dir() else 0,
                "max_channels": max_channels,
                "current_active": calls_waiting + busy_agents if max_channels > 0 else 0,
                "channels_available": max(0, max_channels - (calls_waiting + busy_agents)) if max_channels > 0 else "unlimited"
            },
            
            # Configuración activa
            "config": dialer_config,
            
            # Predicción (si disponible)
            "prediction": prediction,
            
            # Lista de agentes disponibles (para asignación)
            "available_agents_list": [
                m.get("name") for m in members.get("details", [])
                if not m.get("paused") and m.get("status") in [1, 2]  # Available states
            ] if members.get("details") else []
        }
    
    def _calculate_hash(self, data: Dict) -> str:
        """Calcula hash de los datos para detectar cambios"""
        return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()
    
    async def monitor_loop(self):
        """Loop principal de monitoreo"""
        self.ami = AsteriskAMI()
        
        while self.running:
            try:
                if not self.ami.connected:
                    if not await self.ami.connect():
                        logger.error("No se pudo conectar a AMI, reintentando en 5s...")
                        await asyncio.sleep(5)
                        continue
                
                # Obtener estado de colas
                queues = await self.ami.get_queues()
                
                # Registrar en analytics
                if self.analytics and queues:
                    self.analytics.record_snapshot(queues)
                
                # Preparar datos para broadcast
                queues_data = [q.to_dict() for q in queues]
                
                # Obtener analytics si hay datos suficientes
                analytics_data = None
                if self.analytics and queues:
                    try:
                        anomalies = self.analytics.detect_anomalies(queues)
                        suggestions = self.analytics.get_optimization_suggestions(queues)
                        analytics_data = {
                            "anomalies": anomalies,
                            "suggestions": suggestions
                        }
                    except Exception as e:
                        logger.debug(f"Error calculando analytics: {e}")
                
                current_data = {
                    "timestamp": datetime.now().isoformat(),
                    "queues": queues_data,
                    "total_queues": len(queues),
                    "total_waiting": sum(q.calls for q in queues),
                    "total_available": sum(q.available_members for q in queues),
                    "total_busy": sum(q.busy_members for q in queues),
                    "analytics": analytics_data
                }
                
                # Solo enviar si hay cambios
                current_hash = self._calculate_hash(current_data)
                if current_hash != self.last_hash or not self.clients:
                    self.last_data = current_data
                    self.last_hash = current_hash
                    
                    await self.broadcast({
                        "type": "queue_update",
                        "data": current_data
                    })
                    
                    # Guardar en Redis
                    if self.redis_client:
                        try:
                            self.redis_client.set(
                                "asterisk:queues:current",
                                json.dumps(current_data),
                                ex=60
                            )
                        except:
                            pass
                
                await asyncio.sleep(MONITOR_INTERVAL)
                
            except Exception as e:
                logger.error(f"Error en monitor loop: {e}")
                await asyncio.sleep(5)
                # Reintentar conexión
                if self.ami:
                    await self.ami.disconnect()
    
    async def start(self):
        """Inicia el servidor WebSocket"""
        if not WEBSOCKETS_AVAILABLE:
            logger.error("websockets no está instalado. Ejecuta: pip install websockets")
            return
        
        self.running = True
        
        # Crear contexto SSL si está configurado
        ssl_context = self._create_ssl_context()
        protocol = "wss" if ssl_context else "ws"
        
        # Iniciar servidor WebSocket
        server = await websockets.serve(
            self.handle_client,
            self.host,
            self.port,
            ssl=ssl_context,
            ping_interval=30,
            ping_timeout=10
        )
        
        logger.info(f"🚀 Servidor WebSocket iniciado en {protocol}://{self.host}:{self.port}")
        
        # Iniciar loop de monitoreo
        monitor_task = asyncio.create_task(self.monitor_loop())
        
        try:
            await asyncio.gather(
                server.wait_closed(),
                monitor_task
            )
        except asyncio.CancelledError:
            pass
        finally:
            self.running = False
            if self.ami:
                await self.ami.disconnect()
            if self.analytics:
                self.analytics._save_model()
            server.close()
            await server.wait_closed()
    
    async def stop(self):
        """Detiene el servidor"""
        self.running = False


def print_queue_status(queues: List[QueueStats]):
    """Imprime el estado de las colas de forma legible"""
    print("\n" + "="*80)
    print(f"📊 MONITOREO DE COLAS - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    if not queues:
        print("⚠️  No hay colas configuradas o no se pudo obtener información")
        return
    
    for queue in queues:
        print(f"\n📞 Cola: {queue.name}")
        print(f"   Estrategia: {queue.strategy}")
        print(f"   Llamadas en espera: {queue.calls}")
        print(f"   Tiempo promedio espera: {queue.holdtime}s")
        print(f"   Tiempo promedio conversación: {queue.talk_time}s")
        print(f"   Completadas: {queue.completed} | Abandonadas: {queue.abandoned}")
        print(f"   Nivel de servicio: {queue.service_level_perf:.1f}%")
        
        # Miembros
        print(f"\n   👥 Miembros ({len(queue.members)} total):")
        print(f"      ✅ Disponibles: {queue.available_members}")
        print(f"      📞 Ocupados: {queue.busy_members}")
        print(f"      ⏸️  En pausa: {queue.paused_members}")
        
        if queue.members:
            print("\n   Detalle de agentes:")
            for member in queue.members:
                status_icon = "🟢" if member.status == 1 and not member.paused else "🔴" if member.paused else "🟡"
                pause_text = f" (PAUSA: {member.paused_reason})" if member.paused else ""
                print(f"      {status_icon} {member.interface} - {member.status_text}{pause_text} - Llamadas: {member.calls_taken}")
        
        # Llamadas en espera
        if queue.callers:
            print(f"\n   📋 Llamadas en espera ({len(queue.callers)}):")
            for caller in sorted(queue.callers, key=lambda x: x.position):
                print(f"      #{caller.position} - {caller.caller_id_num} ({caller.caller_id_name}) - Esperando: {caller.wait_formatted}")
    
    print("\n" + "="*80)


async def run_websocket_server(host: str = WS_HOST, port: int = WS_PORT,
                                ssl_cert: str = None, ssl_key: str = None):
    """Ejecuta el servidor WebSocket"""
    server = QueueWebSocketServer(host, port, ssl_cert, ssl_key)
    
    try:
        await server.start()
    except KeyboardInterrupt:
        logger.info("🛑 Deteniendo servidor...")
        await server.stop()


async def monitor_queues_cli(interval: int = MONITOR_INTERVAL):
    """Monitoreo por CLI (sin WebSocket)"""
    ami = AsteriskAMI()
    analytics = QueueAnalytics()
    
    try:
        if not await ami.connect():
            logger.error("No se pudo conectar a AMI. Verifica la configuración.")
            return
        
        print("\n🚀 Iniciando monitoreo de colas de Asterisk...")
        print(f"   Host: {AMI_HOST}:{AMI_PORT}")
        print(f"   Intervalo: {interval} segundos")
        print("   Presiona Ctrl+C para detener\n")
        
        while True:
            try:
                queues = await ami.get_queues()
                analytics.record_snapshot(queues)
                print_queue_status(queues)
                
                # Mostrar anomalías detectadas
                anomalies = analytics.detect_anomalies(queues)
                if anomalies:
                    print("\n⚠️  ANOMALÍAS DETECTADAS:")
                    for a in anomalies:
                        print(f"   [{a['severity'].upper()}] {a['queue']}: {a['message']}")
                
                # Mostrar sugerencias
                suggestions = analytics.get_optimization_suggestions(queues)
                if suggestions:
                    print("\n💡 SUGERENCIAS:")
                    for s in suggestions:
                        print(f"   [{s['priority'].upper()}] {s['queue']}: {s['message']}")
                
                await asyncio.sleep(interval)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Error obteniendo estado de colas: {e}")
                await asyncio.sleep(interval)
                
    finally:
        await ami.disconnect()
        analytics._save_model()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Monitor de Colas de Asterisk con WebSocket")
    parser.add_argument("-m", "--mode", choices=["ws", "cli"], default="ws",
                        help="Modo de ejecución: 'ws' para WebSocket server, 'cli' para consola")
    parser.add_argument("-i", "--interval", type=int, default=MONITOR_INTERVAL,
                        help=f"Intervalo de actualización en segundos (default: {MONITOR_INTERVAL})")
    parser.add_argument("--ws-host", default=WS_HOST,
                        help=f"Host para WebSocket server (default: {WS_HOST})")
    parser.add_argument("--ws-port", type=int, default=WS_PORT,
                        help=f"Puerto para WebSocket server (default: {WS_PORT})")
    parser.add_argument("--ssl-cert", default=None,
                        help="Ruta al certificado SSL para WSS")
    parser.add_argument("--ssl-key", default=None,
                        help="Ruta a la llave privada SSL para WSS")
    parser.add_argument("--ami-host", default=AMI_HOST,
                        help=f"Host de Asterisk AMI (default: {AMI_HOST})")
    parser.add_argument("--ami-port", type=int, default=AMI_PORT,
                        help=f"Puerto de Asterisk AMI (default: {AMI_PORT})")
    parser.add_argument("-u", "--username", default=AMI_USERNAME,
                        help=f"Usuario AMI (default: {AMI_USERNAME})")
    parser.add_argument("-p", "--password", default=AMI_SECRET,
                        help="Contraseña AMI")
    
    args = parser.parse_args()
    
    # Actualizar configuración global
    AMI_HOST = args.ami_host
    AMI_PORT = args.ami_port
    AMI_USERNAME = args.username
    AMI_SECRET = args.password
    MONITOR_INTERVAL = args.interval
    
    if args.mode == "ws":
        # Modo WebSocket Server
        asyncio.run(run_websocket_server(
            host=args.ws_host,
            port=args.ws_port,
            ssl_cert=args.ssl_cert,
            ssl_key=args.ssl_key
        ))
    else:
        # Modo CLI
        asyncio.run(monitor_queues_cli(interval=args.interval))

