# BancoCloud · DSY1107 · Evaluación Parcial 5

Aplicación bancaria con React, TypeScript y microservicios Java 21 con Spring Boot. Un **Cliente** solicita cuentas y consulta su estado. Un **Empleado** revisa solicitudes, habilita cuentas y administra la mensajería. Amazon Cognito emite el JWT que el frontend adjunta y el backend valida.

## Arquitectura

- **AWS.** Amplify aloja el frontend. API Gateway dirige las peticiones al balanceador `BancoCloud-ALB`, que las entrega a `banco-bff` en EC2. Los microservicios usan MySQL en RDS.
- **RabbitMQ.** Mantiene el flujo operativo de solicitudes. El panel de Empleado permite enviar mensajes de prueba y administrar colas, exchanges y bindings. Los consumidores registran errores y derivan los mensajes fallidos a su DLQ.
- **Kafka.** Las solicitudes creadas y las cuentas habilitadas generan eventos. `kafka-service` los consume, registra métricas en MySQL y ofrece el panel de administración a través del BFF.

| Tópico Kafka | Grupo consumidor | Retención |
|---|---|---:|
| `bancocloud.solicitudes.creadas.v1` | `bancocloud-analitica-solicitudes-v1` | 7 días |
| `bancocloud.cuentas.habilitadas.v1` | `bancocloud-analitica-cuentas-v1` | 30 días |

Los tópicos de negocio tienen **3 particiones** y usan `cleanup.policy=delete`. Docker Compose levanta **3 brokers en local**, con factor de replicación `3` y `min.insync.replicas=2`. En AWS se verificó la integración en una **EC2 `t3.small` con un broker Kafka en modo KRaft**, factor de replicación `1` y `min.insync.replicas=1`.

## Cómo levantar BancoCloud en local

Necesitas Docker Compose, Java 21, Node.js 22.12 o superior y una cuenta de Cognito. El listado administrativo de usuarios también requiere credenciales AWS con acceso al pool de Cognito.

### 1. Levanta Docker

En la raíz del proyecto, crea `.env` a partir de [`.env.example`](.env.example) si todavía no existe. Define allí el usuario y la contraseña de RabbitMQ. Después ejecuta:

~~~powershell
docker compose --profile rabbitmq-cluster up -d --wait rabbitmq-cluster-1 rabbitmq-cluster-2 rabbitmq-cluster-3
docker compose --profile kafka up -d --wait --wait-timeout 240 kafka-ui
docker start bancocloud-mysql-local
~~~

El primer comando inicia los tres nodos de RabbitMQ. El segundo inicia ZooKeeper, tres brokers de Kafka y [Kafka UI](http://localhost:8085/). El último inicia la base de pruebas en el puerto `3307`.

<details>
<summary>Si todavía no tienes el contenedor MySQL local</summary>

En la raíz, crea `.env.mysql-local` con los valores de abajo. Elige tus propias contraseñas de prueba.

~~~dotenv
MYSQL_DATABASE=bancocloud_local
MYSQL_USER=usuario_local
MYSQL_PASSWORD=reemplaza_por_una_clave_local
MYSQL_ROOT_PASSWORD=reemplaza_por_otra_clave_local
~~~

Ejecuta este comando una sola vez en lugar de `docker start bancocloud-mysql-local`:

~~~powershell
docker run -d --name bancocloud-mysql-local -p 127.0.0.1:3307:3306 --env-file .env.mysql-local -v bancocloud-mysql-local-data:/var/lib/mysql mysql:8.4
~~~

</details>

### 2. Guarda la configuración del backend

Crea `application-local.properties` en la **raíz del proyecto**, al lado del Compose. Cambia los usuarios y las contraseñas por los de tus contenedores locales. Los de RabbitMQ son los mismos que pusiste en `.env`.

~~~properties
DB_URL=jdbc:mysql://127.0.0.1:3307/bancocloud_local?sslMode=DISABLED&allowPublicKeyRetrieval=true
DB_USERNAME=tu_usuario_mysql
DB_PASSWORD=tu_clave_mysql
RABBITMQ_USERNAME=tu_usuario_rabbitmq
RABBITMQ_PASSWORD=tu_clave_rabbitmq
RABBITMQ_PORT=5673
RABBITMQ_MANAGEMENT_URL=http://127.0.0.1:15673
KAFKA_SOLICITUDES_ENABLED=true
KAFKA_CUENTAS_ENABLED=true
server.address=127.0.0.1
~~~

Los microservicios comparten este archivo y se conectan a los brokers locales en los puertos `9092`, `9093` y `9094`. Git ignora los archivos de configuración local.

### 3. Inicia los microservicios

Abre una terminal PowerShell por módulo, entra a su carpeta y ejecuta estas dos líneas:

~~~powershell
$env:SPRING_CONFIG_ADDITIONAL_LOCATION = 'file:../application-local.properties'
.\mvnw.cmd spring-boot:run
~~~

| Carpeta | Comprobación |
|---|---|
| `cuentas-service` | [Estado en el puerto 8081](http://localhost:8081/actuator/health) |
| `solicitudes-service` | [Estado en el puerto 8082](http://localhost:8082/actuator/health) |
| `kafka-service` | [Estado en el puerto 8083](http://localhost:8083/actuator/health) |
| `banco-bff` | [Estado en el puerto 8080](http://localhost:8080/actuator/health) |

Cada enlace debe mostrar `UP`. Si un puerto ya está ocupado, revisa su enlace antes de iniciar otra instancia.

### 4. Abre el frontend

En otra terminal PowerShell, desde la raíz del proyecto:

~~~powershell
Set-Location banco-frontend
$env:VITE_API_BASE_URL = 'http://localhost:8080'
npm.cmd ci
npm.cmd run dev -- --strictPort
~~~

Abre [BancoCloud local](http://localhost:5173/). Para comprobar el build, ejecuta `npm.cmd run build` desde `banco-frontend`.

## Guía de revisión de la Evaluación Parcial 5

| Indicador | Dónde revisarlo |
|---|---|
| **Configuración del cliente Kafka (12%)** | [Docker Compose](docker-compose.yml) y propiedades de [solicitudes](solicitudes-service/src/main/resources/application.properties), [cuentas](cuentas-service/src/main/resources/application.properties) y [Kafka](kafka-service/src/main/resources/application.properties). Incluyen brokers, serialización, grupos y confirmación de mensajes. |
| **DTO, productores y consumidores (12% + 14% + 14%)** | El [productor de solicitudes](solicitudes-service/src/main/java/cl/duoc/solicitudes_service/messaging/kafka/SolicitudKafkaPublisher.java) y el [productor de cuentas](cuentas-service/src/main/java/cl/duoc/cuentas_service/messaging/kafka/CuentaKafkaPublisher.java) registran el evento tipado en una tabla de pendientes junto con el cambio de negocio y lo envían como JSON. Los [listeners](kafka-service/src/main/java/cl/duoc/kafka_service/eventos/ConsumidoresKafka.java) delegan el procesamiento a [AnaliticaKafka](kafka-service/src/main/java/cl/duoc/kafka_service/analitica/AnaliticaKafka.java), que deduplica por `eventId`. |
| **Administración de tópicos y grupos (6% + 6%)** | Panel **Empleado → Kafka** y [API administrativa](kafka-service/src/main/java/cl/duoc/kafka_service/admin/KafkaAdminController.java). El panel permite crear, listar, cambiar la retención y eliminar tópicos, además de consultar grupos. La API permite aumentar particiones, restablecer configuraciones y administrar offsets de grupos inactivos. |
| **Manejo de fallos (12%)** | [Configuración de reintentos y DLT](kafka-service/src/main/java/cl/duoc/kafka_service/config/KafkaInfraestructura.java). Los mensajes inválidos van al tópico `.DLT` de su flujo. Los fallos transitorios admiten dos reintentos y los logs permiten seguir el evento. |
| **Parámetros de tópicos (12%)** | La pestaña **Tópicos** y Kafka UI muestran retención, líderes, particiones y réplicas sincronizadas. El servicio `kafka-topics` del Compose configura los tópicos de negocio para la prueba local. |
| **Grupos y reparto de carga (12%)** | En **Grupos**, revisa miembros, offsets y lag. Para probar el rebalanceo en local, inicia una segunda instancia de `kafka-service` con la misma configuración y `KAFKA_SERVICE_PORT=8084`. Observa cómo se reparten las particiones y cómo vuelven al primer consumidor al detener la segunda instancia. |

## Demostración y resultados

1. Crea una solicitud como **Cliente** y apruébala como **Empleado**. En **Kafka → Métricas**, comprueba que aparezcan **Solicitudes creadas** y **Cuentas habilitadas**.
2. En **Grupos**, comprueba que el offset confirmado alcance al offset final y que el lag sea `0` en las particiones con eventos. En **RabbitMQ**, verifica que carguen colas, exchanges y bindings.
3. En **Kafka → Tópicos**, crea un tópico temporal como `bancocloud.ep5.prueba.frontend`. Usa **3 réplicas en local** o **1 réplica en AWS**. Cambia la retención de `86400000` a `172800000` ms y elimina ese tópico. El contador debe volver a su valor inicial.

**Resultado en local.** Se verificaron los dos eventos de negocio, el rebalanceo y la creación, el cambio de retención y la eliminación de un tópico temporal.

**Resultado en AWS.** Los cuatro servicios respondieron `UP`. El [panel publicado en Amplify](https://main.d2anm3z9q7k9u4.amplifyapp.com/) mostró **2 eventos procesados de Cuenta Ahorro**: una solicitud creada y una cuenta habilitada. También mostró **4 tópicos y 2 grupos de consumidores**. Para acceder al panel de Kafka, inicia sesión con el rol **Empleado**.

Las [pruebas de integración](kafka-service/src/test/java/cl/duoc/kafka_service/KafkaServiceIntegrationTest.java) se ejecutan con `.\mvnw.cmd test` desde `kafka-service`. Cubren consumo, deduplicación, reintentos, DLT, administración y roles. En Windows, el borrado se comprueba con el clúster de Docker porque ese caso se omite en Kafka embebido.
