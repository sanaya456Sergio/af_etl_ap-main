# Bitácora de Transformación y Migración de Datos de Áreas Protegidas SINAP a LADM_COL-AP

---

## Datos Generales

- **Fecha de inicio:** 2024-10-21  
- **Autores:** Leo Cardona y Cesar Alfonso Basurto (CEICOL SAS)  
- **Correo:** contacto@ceicol.com  
- **Propósito:**  
  Integrar y homologar la información de áreas protegidas provenientes del SINAP en una estructura intermedia acorde al estándar del modelo LADM_COL-AP, garantizando la calidad, consistencia y trazabilidad del proceso ETL.

---

## 1. Preparación del Entorno

- **Establecimiento de Esquema:**  
  Se configuró el `search_path` para incluir el esquema `estructura_intermedia` y `public`, lo que facilita el manejo ordenado de la información y evita ambigüedades en la referencia de tablas.

---

## 2. Migración y Homologación de Áreas Protegidas

### 2.1 Inserción en `ap_uab_areaprotegida`

- **Origen de Datos:**  
  Los datos se extraen de las tablas `insumos.area_protegida`(shp-RUNAP) y `insumos.información_runap_excel`(Reporte-Dane) mediante un `JOIN` que vincula la información geométrica y administrativa.

- **Transformaciones Clave:**

  - **Identificación y Nomenclatura:**  
    Se genera un identificador único concatenando el prefijo `'AP_'` con un número incremental obtenido con `row_number()`.

  - **Homologación de Categoría:**  
    Se transforma el valor original del campo `ap_categor` a una categoría estándar en el modelo de destino. La siguiente tabla resume la homologación:

    | Categoría Original                                  | Categoría Homologada                                                   |
    |-----------------------------------------------------|------------------------------------------------------------------------|
    | Reserva Natural de la Sociedad Civil                | Privado.Reserva_Natural_Sociedad_Civil                                 |
    | Parque Nacional Natural                             | Publico.Sistema_Parques_Nacionales_Naturales.Parque_Nacional             |
    | Parques Naturales Regionales                        | Publico.Parque_Natural_Regional                                        |
    | Vía Parque                                          | Publico.Sistema_Parques_Nacionales_Naturales.Via_Parque                  |
    | Área Natural Única                                  | Publico.Sistema_Parques_Nacionales_Naturales.Area_Natural_Unica          |
    | Distritos de Conservación de Suelos                 | Publico.Distrito_Conservacion_Suelos                                   |
    | Distritos Nacionales de Manejo Integrado            | Publico.Distrito_Manejo_Integrado                                      |
    | Distritos Regionales de Manejo Integrado            | Publico.Distrito_Manejo_Integrado                                      |
    | Áreas de Recreación                                 | Publico.Area_Recreacion                                                |
    | Reserva Natural                                     | Publico.Sistema_Parques_Nacionales_Naturales.Reserva_Natural             |
    | Reservas Forestales Protectoras Nacionales        | Publico.Reserva_Forestal_Protectora                                    |
    | Reservas Forestales Protectoras Regionales        | Publico.Reserva_Forestal_Protectora                                    |
    | Santuario de Flora                                  | Publico.Sistema_Parques_Nacionales_Naturales.Santuario_Flora             |
    | Santuario de Fauna                                  | Publico.Sistema_Parques_Nacionales_Naturales.Santuario_Fauna             |
    | Santuario de Fauna y Flora                          | Publico.Sistema_Parques_Nacionales_Naturales.Santuario_Flora_y_Fauna       |

  - **Ámbito de Gestión:**  
    Se asigna el valor del ámbito de gestión en función del contenido del campo `"Ámbito de gestión"`. La siguiente tabla detalla la homologación:

    | Ámbito de Gestión Original                      | Ámbito Homologado |
    |-------------------------------------------------|-------------------|
    | Valor que contenga la palabra *privada*         | Local             |
    | Valor que contenga *protegidas nacionales*      | Nacional          |
    | Valor que contenga *protegidas regional*        | Regional          |

  - **Estado de Inscripción:**  
    - Si el campo `condicion` es `'inscrita'`, si tiene fecha de resgistro se asigna registrada si no se asigna el estado **Inscrita** o se le asigna este valor si ya venia ocn él.  
    - Si el campo `condicion` es `'registrada'` (después de aplicar `TRIM` y `LOWER`), se asigna **Registrada**.

  - **Conversión de Fechas:**  
    Se utiliza `NULLIF` para convertir valores inválidos (como `'NaT'`) a `NULL` y luego se transforman al tipo `date`.

  - **Normalización de Áreas:**  
    Se aplican funciones `COALESCE` para asegurar que los campos de área total, terrestre y marítima tengan valores numéricos, asignando el valor 0 en ausencia de datos.

  - **Transformación de Información Administrativa:**  
    Se construye el campo `fuente_administrativa_nombre` concatenando:
    - El tipo de acto administrativo.  
    - El número del acto.  
    - El año extraído de la fecha del acto.
    
    Además, se asignan valores constantes en los campos:
    - `interesado_tipo_interesado`: `'Persona_Juridica'`
    - `interesado_tipo_documento`: `'NIT'`
    - `fuente_administrativa_tipo`: `'Documento_Publico.'`
    - `fuente_administrativa_estado_disponibilidad`: `'Disponible'`  
    Para el campo `interesado_numero_documento`, se aplica un formateo específico que incorpora separadores de miles.

---

### 2.2 Procesamiento de la Unidad Espacial en `ap_ue_areaprotegida`

- **Transformación Geométrica:**  
  Se utiliza la función `ST_Force3D` para convertir la geometría a formato tridimensional y `ST_Transform` para reproyectarla al sistema de referencia espacial SRID 9377.

- **Gestión de Valores de Área:**  
  Se aplican funciones `COALESCE` para asegurar la consistencia de los valores en los campos de áreas total, terrestre y marítima, gestionando de forma adecuada la ausencia de datos.

---

## 3. Homologación de Fuentes Administrativas

### 3.1 Homologación de Actos y Resoluciones

Dentro del proceso de homologación de fuentes administrativas se realizó una especial atención a la transformación de actos y resoluciones. Esto se llevó a cabo de la siguiente manera:

- **Extracción del Tipo de Acto:**  
  Se identificaron los siguientes patrones en el campo `fuente_administrativa_nombre` para estandarizar el tipo de acto:
  
  | Patrón Detectado en el Texto          | Tipo Homologado |
  |---------------------------------------|-----------------|
  | `acuerdo%` o `acuerdos%`               | Acuerdo         |
  | `resolución%`                         | Resolucion      |
  | `decreto%`                            | Decreto         |
  | Otros                                 | Se toma el primer término de la cadena |

- **Captura del Número del Acto:**  
  Se usaron expresiones regulares para identificar y extraer el número del acto. Se manejan dos escenarios:
  
  - Cuando el nombre del acto contiene términos como `Directivo`, `Directiva` o `Conjunta`.
  - Cuando el nombre del acto es de tipo `Acuerdo`, `Acuerdos`, `Resolución` o `Decreto`.
  
  En ambos casos, se extrae el número del acto de forma precisa y se convierte a un valor numérico.

- **Extracción del Año del Acto:**  
  Se analiza la cadena para buscar el año, usualmente situado después de la palabra “de” seguida de cuatro dígitos. Así se extrae y convierte el año a un valor numérico.

### 3.2 Truncado e Inserción en `ap_fuenteadministrativa`

- **Inicialización de la Tabla:**  
  Se ejecuta un `TRUNCATE` para limpiar la tabla y garantizar una carga limpia sin registros previos.

- **Proceso de Inserción:**  
  Utilizando la homologación descrita en la sección anterior, se insertan en la tabla `ap_fuenteadministrativa` los siguientes campos:
  - `uab_identificador`
  - `fuente_administrativa_fecha_documento_fuente`
  - `fuente_administrativa_tipo_formato` (resultado de la homologación de actos y resoluciones)
  - `fuente_administrativa_numero`
  - `fuente_administrativa_anio`

---

## 4. Validaciones y Consideraciones Finales

- **Integridad y Consistencia:**
  - Se verificó la consistencia en las relaciones entre tablas, en particular entre `area_protegida` y la información complementaria.
  - Los registros con datos incompletos o inconsistentes fueron identificados para su corrección o exclusión.

- **Estandarización de Datos:**  
  - Se implementó la creación de identificadores únicos para evitar duplicidades y facilitar la trazabilidad.  
  - Se emplearon funciones SQL (como `COALESCE`, `NULLIF`, `TRIM` y `LOWER`) para la normalización y la transformación uniforme de la información.

- **Manejo de Geometrías:**  
  Todas las geometrías fueron transformadas a formato tridimensional y reproyectadas al SRID 9377, asegurando el cumplimiento de los estándares requeridos para el modelo.

---
