-- ============================
-- TABLAS DIMENSIONALES
-- ============================

CREATE TABLE Maquinas (
    id_maquina SERIAL PRIMARY KEY,
    nombre VARCHAR(50) NOT NULL
);

CREATE TABLE Operarios (
    id_operario SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL
);

CREATE TABLE Referencias (
    id_referencia SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL
);

-- ============================
-- TABLA DE HECHOS
-- ============================

CREATE TABLE RegistrosProduccion (
    id_registro SERIAL PRIMARY KEY,
    fecha DATE NOT NULL,
    mes VARCHAR(15),
    año INT,
    maquina_id INT REFERENCES Maquinas(id_maquina),
    operario_id INT REFERENCES Operarios(id_operario),
    referencia_id INT REFERENCES Referencias(id_referencia),
    pacas_producidas NUMERIC(10,2),
    horas_trabajadas NUMERIC(5,2),
    horas_no_trabajadas NUMERIC(5,2),
    turno VARCHAR(50),
    tiempo_total_paros NUMERIC(5,2),
    observaciones TEXT
);

-- ============================
-- TABLA DE DETALLE DE PAROS
-- ============================

CREATE TABLE DetalleParosProduccion (
    id_paro SERIAL PRIMARY KEY,
    registro_id INT REFERENCES RegistrosProduccion(id_registro) ON DELETE CASCADE,
    codigo_paro VARCHAR(20),
    subcodigo VARCHAR(50),
    tipo_paro VARCHAR(100),
    horas_paro NUMERIC(5,2),
    area_involucrada VARCHAR(50),
    personal_involucrado VARCHAR(100),
    observaciones_paro TEXT
);

-- ============================
-- TRIGGER DE INTEGRIDAD (opcional)
-- ============================

CREATE OR REPLACE FUNCTION actualizar_tiempo_paros()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE RegistrosProduccion
    SET tiempo_total_paros = (
        SELECT SUM(horas_paro)
        FROM DetalleParosProduccion
        WHERE registro_id = NEW.registro_id
    )
    WHERE id_registro = NEW.registro_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_actualizar_tiempo_paros
AFTER INSERT OR UPDATE OR DELETE ON DetalleParosProduccion
FOR EACH ROW
EXECUTE FUNCTION actualizar_tiempo_paros();