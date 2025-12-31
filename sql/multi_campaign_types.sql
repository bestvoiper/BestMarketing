-- =============================================================================
-- SQL para Sistema Multi-Tipo de Campañas
-- Soporta: Audio, Discador, WhatsApp, Facebook, Telegram, Email, SMS
-- =============================================================================

-- Agregar columnas a tabla campanas para soportar múltiples tipos
-- Ejecutar estos comandos uno por uno si hay errores

-- 1. Agregar columna de tipo de campaña
ALTER TABLE campanas 
ADD COLUMN IF NOT EXISTS tipo VARCHAR(20) DEFAULT 'Audio' 
COMMENT 'Tipo de campaña: Audio, Discador, WhatsApp, Facebook, Telegram, Email, SMS';

-- 2. Columnas específicas para Discador
ALTER TABLE campanas 
ADD COLUMN IF NOT EXISTS cola_destino VARCHAR(50) DEFAULT '5000'
COMMENT 'Extension de la cola de agentes para transferencia';

ALTER TABLE campanas 
ADD COLUMN IF NOT EXISTS contexto VARCHAR(50) DEFAULT 'default'
COMMENT 'Contexto de FreeSWITCH para el transfer';

ALTER TABLE campanas 
ADD COLUMN IF NOT EXISTS overdial_ratio DECIMAL(3,2) DEFAULT 1.20
COMMENT 'Ratio de sobrediscado (1.20 = 20% más llamadas que agentes)';

ALTER TABLE campanas 
ADD COLUMN IF NOT EXISTS max_abandon_rate DECIMAL(5,2) DEFAULT 3.00
COMMENT 'Porcentaje máximo de abandonos permitido';

-- 3. Columnas específicas para mensajería
ALTER TABLE campanas 
ADD COLUMN IF NOT EXISTS mensaje_template TEXT DEFAULT NULL
COMMENT 'Template del mensaje para WhatsApp, Telegram, etc';

ALTER TABLE campanas 
ADD COLUMN IF NOT EXISTS api_config JSON DEFAULT NULL
COMMENT 'Configuración de API para canales externos (WhatsApp, Facebook, etc)';

-- 4. Columnas de métricas
ALTER TABLE campanas 
ADD COLUMN IF NOT EXISTS fecha_inicio DATETIME DEFAULT NULL
COMMENT 'Timestamp de inicio de la campaña';

ALTER TABLE campanas 
ADD COLUMN IF NOT EXISTS fecha_fin DATETIME DEFAULT NULL
COMMENT 'Timestamp de fin de la campaña';

ALTER TABLE campanas 
ADD COLUMN IF NOT EXISTS duracion_total INT DEFAULT 0
COMMENT 'Duración total en segundos';

ALTER TABLE campanas 
ADD COLUMN IF NOT EXISTS total_enviados INT DEFAULT 0
COMMENT 'Total de envíos';

ALTER TABLE campanas 
ADD COLUMN IF NOT EXISTS total_completados INT DEFAULT 0
COMMENT 'Total de completados';

-- 5. Crear índice para tipo
CREATE INDEX IF NOT EXISTS idx_campanas_tipo ON campanas(tipo);

-- 6. Actualizar campañas existentes como tipo Audio
UPDATE campanas SET tipo = 'Audio' WHERE tipo IS NULL OR tipo = '';


-- =============================================================================
-- TABLA DE AGENTES (para Discador)
-- =============================================================================
CREATE TABLE IF NOT EXISTS agentes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    extension VARCHAR(20) NOT NULL,
    nombre VARCHAR(100),
    cola VARCHAR(50) NOT NULL DEFAULT '5000',
    estado ENUM('disponible', 'en_llamada', 'pausa', 'desconectado') DEFAULT 'desconectado',
    activo ENUM('S', 'N') DEFAULT 'S',
    ultimo_cambio DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    tiempo_en_estado INT DEFAULT 0 COMMENT 'Segundos en el estado actual',
    llamadas_atendidas INT DEFAULT 0,
    tiempo_conversacion INT DEFAULT 0 COMMENT 'Tiempo total de conversación en segundos',
    
    INDEX idx_agentes_cola (cola),
    INDEX idx_agentes_estado (estado),
    INDEX idx_agentes_activo (activo),
    UNIQUE INDEX idx_agentes_extension (extension)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Agentes para campañas de discador';


-- =============================================================================
-- TABLA DE COLAS (para Discador)
-- =============================================================================
CREATE TABLE IF NOT EXISTS colas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    extension VARCHAR(20) NOT NULL,
    nombre VARCHAR(100),
    estrategia ENUM('round-robin', 'longest-idle', 'ring-all', 'random') DEFAULT 'longest-idle',
    timeout_ring INT DEFAULT 20 COMMENT 'Segundos de ring antes de ir al siguiente',
    max_wait INT DEFAULT 300 COMMENT 'Tiempo máximo de espera en cola',
    activa ENUM('S', 'N') DEFAULT 'S',
    musica_espera VARCHAR(100) DEFAULT 'default',
    anuncio_posicion ENUM('S', 'N') DEFAULT 'N',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_colas_activa (activa),
    UNIQUE INDEX idx_colas_extension (extension)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Colas de agentes para discador';


-- =============================================================================
-- TABLA DE HISTORIAL DE LLAMADAS DISCADOR
-- =============================================================================
CREATE TABLE IF NOT EXISTS dialer_call_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    uuid VARCHAR(100) NOT NULL,
    campaign VARCHAR(100) NOT NULL,
    telefono VARCHAR(30) NOT NULL,
    estado VARCHAR(10),
    fecha_envio DATETIME,
    fecha_respuesta DATETIME,
    fecha_transfer DATETIME,
    fecha_fin DATETIME,
    duracion_total INT DEFAULT 0 COMMENT 'Duración total en segundos',
    duracion_agente INT DEFAULT 0 COMMENT 'Tiempo con agente en segundos',
    agente_extension VARCHAR(20),
    hangup_cause VARCHAR(100),
    amd_resultado VARCHAR(20) COMMENT 'Resultado AMD: HUMAN, MACHINE, NOTSURE',
    intentos INT DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_dialer_history_campaign (campaign),
    INDEX idx_dialer_history_telefono (telefono),
    INDEX idx_dialer_history_estado (estado),
    INDEX idx_dialer_history_fecha (fecha_envio),
    INDEX idx_dialer_history_uuid (uuid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Historial de llamadas del marcador predictivo';


-- =============================================================================
-- TABLA DE MÉTRICAS EN TIEMPO REAL
-- =============================================================================
CREATE TABLE IF NOT EXISTS dialer_realtime_stats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    campaign VARCHAR(100) NOT NULL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    -- Llamadas
    calls_dialing INT DEFAULT 0,
    calls_ringing INT DEFAULT 0,
    calls_in_queue INT DEFAULT 0,
    calls_with_agent INT DEFAULT 0,
    calls_total_active INT DEFAULT 0,
    
    -- Métricas
    cps_current DECIMAL(5,2) DEFAULT 0,
    overdial_ratio DECIMAL(3,2) DEFAULT 1.20,
    abandon_rate DECIMAL(5,2) DEFAULT 0,
    
    -- Agentes
    agents_total INT DEFAULT 0,
    agents_available INT DEFAULT 0,
    agents_busy INT DEFAULT 0,
    agents_paused INT DEFAULT 0,
    
    INDEX idx_realtime_campaign (campaign),
    INDEX idx_realtime_timestamp (timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Estadísticas en tiempo real del discador';


-- =============================================================================
-- EJEMPLO: Modificar tabla de campaña existente para Discador
-- =============================================================================
-- Al crear una tabla de campaña de tipo Discador, usar esta estructura:
/*
CREATE TABLE IF NOT EXISTS `nombre_campana_discador` (
    id INT AUTO_INCREMENT PRIMARY KEY,
    telefono VARCHAR(30) NOT NULL,
    nombre VARCHAR(100),
    datos_adicionales JSON,
    
    -- Estados específicos de Discador
    -- P = Procesando/Discando
    -- R = Timbrando
    -- A = Contestada
    -- Q = En Cola (esperando agente)
    -- T = Transferida a agente
    -- C = Completada
    -- N = No Contesta
    -- O = Ocupado
    -- F = Fallo
    -- M = Máquina/Buzón
    -- B = Abandonada (colgó antes del agente)
    -- X = Excluida (DNC)
    -- E = Error
    estado VARCHAR(20) DEFAULT 'pendiente',
    uuid VARCHAR(100),
    
    -- Timestamps
    fecha_envio DATETIME,
    fecha_respuesta DATETIME,
    fecha_transfer DATETIME,
    fecha_agente DATETIME,
    fecha_fin DATETIME,
    
    -- Métricas
    duracion_ring INT DEFAULT 0,
    duracion_cola INT DEFAULT 0,
    duracion_agente INT DEFAULT 0,
    duracion_total INT DEFAULT 0,
    
    -- Control
    intentos INT DEFAULT 0,
    hangup_cause VARCHAR(100),
    amd_resultado VARCHAR(20),
    agente_extension VARCHAR(20),
    
    INDEX idx_telefono (telefono),
    INDEX idx_estado (estado),
    INDEX idx_uuid (uuid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
*/


-- =============================================================================
-- INSERTAR COLA POR DEFECTO
-- =============================================================================
INSERT IGNORE INTO colas (extension, nombre, estrategia)
VALUES ('5000', 'Cola Principal', 'longest-idle');


-- =============================================================================
-- VISTA RESUMEN DE CAMPAÑAS
-- =============================================================================
CREATE OR REPLACE VIEW v_campanas_resumen AS
SELECT 
    c.nombre,
    c.tipo,
    c.activo,
    c.cps,
    c.reintentos,
    c.cola_destino,
    c.overdial_ratio,
    c.fecha_inicio,
    c.fecha_fin,
    c.duracion_total,
    (SELECT COUNT(*) FROM agentes a WHERE a.cola = c.cola_destino AND a.estado = 'disponible') as agentes_disponibles,
    (SELECT COUNT(*) FROM agentes a WHERE a.cola = c.cola_destino AND a.activo = 'S') as agentes_total
FROM campanas c
WHERE c.activo IN ('S', 'P');


-- =============================================================================
-- PROCEDIMIENTO: Crear tabla de campaña Discador
-- =============================================================================
DELIMITER //

CREATE PROCEDURE IF NOT EXISTS crear_tabla_discador(IN p_nombre VARCHAR(100))
BEGIN
    SET @sql = CONCAT('
        CREATE TABLE IF NOT EXISTS `', p_nombre, '` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            telefono VARCHAR(30) NOT NULL,
            nombre VARCHAR(100),
            datos JSON,
            estado VARCHAR(20) DEFAULT ''pendiente'',
            uuid VARCHAR(100),
            fecha_envio DATETIME,
            fecha_respuesta DATETIME,
            fecha_transfer DATETIME,
            fecha_agente DATETIME,
            fecha_fin DATETIME,
            duracion_ring INT DEFAULT 0,
            duracion_cola INT DEFAULT 0,
            duracion_agente INT DEFAULT 0,
            duracion_total INT DEFAULT 0,
            intentos INT DEFAULT 0,
            hangup_cause VARCHAR(100),
            amd_resultado VARCHAR(20),
            agente_extension VARCHAR(20),
            INDEX idx_telefono (telefono),
            INDEX idx_estado (estado),
            INDEX idx_uuid (uuid)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    ');
    
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;


-- =============================================================================
-- TRIGGER: Copiar al historial cuando termina una llamada
-- =============================================================================
-- Nota: Esto requiere crear un trigger por cada tabla de campaña
-- O mejor manejarlo desde la aplicación Python

/*
DELIMITER //

CREATE TRIGGER IF NOT EXISTS trg_after_update_dialer
AFTER UPDATE ON nombre_campana
FOR EACH ROW
BEGIN
    IF NEW.estado IN ('C', 'N', 'O', 'F', 'M', 'B', 'E') AND OLD.estado NOT IN ('C', 'N', 'O', 'F', 'M', 'B', 'E') THEN
        INSERT INTO dialer_call_history (
            uuid, campaign, telefono, estado, 
            fecha_envio, fecha_respuesta, fecha_transfer, fecha_fin,
            duracion_total, duracion_agente, agente_extension, 
            hangup_cause, amd_resultado, intentos
        ) VALUES (
            NEW.uuid, 'nombre_campana', NEW.telefono, NEW.estado,
            NEW.fecha_envio, NEW.fecha_respuesta, NEW.fecha_transfer, NEW.fecha_fin,
            NEW.duracion_total, NEW.duracion_agente, NEW.agente_extension,
            NEW.hangup_cause, NEW.amd_resultado, NEW.intentos
        );
    END IF;
END //

DELIMITER ;
*/


-- =============================================================================
-- EJEMPLO: Insertar agentes de prueba
-- =============================================================================
/*
INSERT INTO agentes (extension, nombre, cola, estado, activo) VALUES
('1001', 'Agente 1', '5000', 'disponible', 'S'),
('1002', 'Agente 2', '5000', 'disponible', 'S'),
('1003', 'Agente 3', '5000', 'disponible', 'S'),
('1004', 'Agente 4', '5000', 'en_llamada', 'S'),
('1005', 'Agente 5', '5000', 'pausa', 'S');
*/


-- =============================================================================
-- FIN DEL SCRIPT
-- =============================================================================
SELECT 'Script ejecutado correctamente' AS resultado;
