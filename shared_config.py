"""
Configuración compartida para todos los procesos del sistema de marcado
Soporta múltiples tipos de campañas: Audio, Discador, WhatsApp, Email, etc.
"""
import logging
import os
from enum import Enum
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool


# =============================================================================
# TIPOS DE CAMPAÑAS SOPORTADAS
# =============================================================================
class CampaignType(Enum):
    """Tipos de campañas soportadas por el sistema"""
    AUDIO = "Audio"           # Campañas de audio masivo (IVR)
    DISCADOR = "Discador"     # Marcador predictivo con agentes (FreeSWITCH)
    WHATSAPP = "WhatsApp"     # Mensajería WhatsApp
    FACEBOOK = "Facebook"     # Mensajería Facebook Messenger
    TELEGRAM = "Telegram"     # Mensajería Telegram
    EMAIL = "Email"           # Campañas de email masivo
    SMS = "SMS"               # Campañas de SMS


# Configuración por tipo de campaña
CAMPAIGN_TYPE_CONFIG = {
    CampaignType.AUDIO: {
        "name": "Audio Masivo",
        "description": "Llamadas automatizadas con reproducción de audio",
        "requires_agents": False,
        "default_cps": 30,
        "max_concurrent": 2000,
    },
    CampaignType.DISCADOR: {
        "name": "Marcador Predictivo", 
        "description": "Marcador con transferencia a agentes via FreeSWITCH",
        "requires_agents": True,
        "default_cps": 15,
        "max_concurrent": 500,
    },
    CampaignType.WHATSAPP: {
        "name": "WhatsApp",
        "description": "Mensajes por WhatsApp Business API",
        "requires_agents": False,
        "default_cps": 50,
        "max_concurrent": 1000,
    },
    CampaignType.EMAIL: {
        "name": "Email Masivo",
        "description": "Campañas de correo electrónico",
        "requires_agents": False,
        "default_cps": 100,
        "max_concurrent": 5000,
    },
}


def get_campaign_type(tipo_str: str):
    """Convierte string de tipo a CampaignType enum"""
    if not tipo_str:
        return None
    tipo_upper = tipo_str.upper()
    for ct in CampaignType:
        if ct.value.upper() == tipo_upper:
            return ct
    return None


# =============================================================================
# CONFIGURACIÓN DE FREESWITCH (Para Audio y Discador)
# =============================================================================
FREESWITCH_HOST = "127.0.0.1"
FREESWITCH_PORT = 8021
FREESWITCH_PASSWORD = "1Pl}0F~~801l"
GATEWAY = "gw_pstn"


# =============================================================================
# CONFIGURACIÓN DE BASE DE DATOS
# =============================================================================
DB_URL = "mysql+pymysql://consultas:consultas@localhost/masivos"


# =============================================================================
# DIRECTORIOS
# =============================================================================
RECORDINGS_DIR = "/var/spool/freeswitch/recordings"


# =============================================================================
# CONFIGURACIÓN DE CONCURRENCIA - AUDIO
# =============================================================================
GLOBAL_MAX_CONCURRENT_CALLS = 2000
CPS_GLOBAL = 30  # Máximo llamadas por segundo


# =============================================================================
# CONFIGURACIÓN DE CONCURRENCIA - DISCADOR
# =============================================================================
DIALER_MAX_CONCURRENT_CALLS = 500
DIALER_CPS_GLOBAL = 15
DIALER_OVERDIAL_RATIO = 1.2  # 20% más llamadas que agentes disponibles
DIALER_MIN_AGENTS = 1  # Mínimo agentes para iniciar marcación
DIALER_ABANDON_THRESHOLD = 5.0  # % máximo de abandonos
DIALER_DEFAULT_QUEUE = "5000"  # Extensión de cola por defecto
DIALER_DEFAULT_CONTEXT = "default"  # Contexto para transfer


# =============================================================================
# CONFIGURACIÓN DE WORKERS
# =============================================================================
STATE_UPDATE_WORKERS = 10
CALL_SENDER_WORKERS = 5
DIALER_SENDER_WORKERS = 3


# =============================================================================
# CACHÉ
# =============================================================================
CACHE_TTL = 10  # Segundos de validez del caché


# =============================================================================
# ESTADOS DE LLAMADAS
# =============================================================================
# Estados terminales comunes (Audio)
TERMINAL_STATES = ('C', 'E', 'O', 'N', 'U', 'R', 'I', 'X', 'T', 'M')

# Estados específicos del Discador
DIALER_STATES = {
    'pendiente': 'Pendiente de marcar',
    'P': 'Procesando/Discando',
    'R': 'Timbrando (Ringing)',
    'A': 'Contestada (Answered)',
    'Q': 'En cola de agentes',
    'T': 'Transferida a agente',
    'C': 'Completada con agente',
    'N': 'No contesta',
    'O': 'Ocupado',
    'F': 'Fallida',
    'M': 'Máquina/Buzón detectado',
    'B': 'Abandonada (colgó en cola)',
    'X': 'Cancelada',
    'E': 'Error de sistema',
}

# Estados terminales del Discador
DIALER_TERMINAL_STATES = ('C', 'N', 'O', 'F', 'M', 'B', 'X', 'E')

# Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def get_logger(name):
    """Obtiene un logger configurado"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    return logger

def create_db_engine(pool_size=100, max_overflow=50):
    """Crea un engine de SQLAlchemy optimizado"""
    return create_engine(
        DB_URL,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_recycle=1800,
        pool_pre_ping=True,
        isolation_level="READ COMMITTED",
        poolclass=QueuePool
    )

# Redis config
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

def is_terminal_state(estado: str) -> bool:
    """Determina si un estado es terminal (libera la plaza de concurrencia)"""
    try:
        return estado is not None and estado.upper() in TERMINAL_STATES
    except Exception:
        return True
