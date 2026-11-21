# BancoCloud

Aplicación bancaria para solicitar y gestionar cuentas. Los clientes pueden crear solicitudes y consultar sus cuentas. Los empleados pueden aprobar o rechazar solicitudes, habilitar cuentas y administrar la mensajería desde el panel.

**Frontend:** React, TypeScript y Vite.  
**Backend:** Java 21 y Spring Boot.  
**Infraestructura:** MySQL, RabbitMQ, Kafka y AWS.

## Arquitectura

[La aplicación publicada en Amplify](https://main.d2anm3z9q7k9u4.amplifyapp.com/) utiliza Amazon Cognito para iniciar sesión. El frontend envía el JWT a API Gateway, que dirige las solicitudes al BFF en EC2 a través de un balanceador. Los microservicios guardan los datos en MySQL sobre Amazon RDS.

| Módulo | Puerto | Responsabilidad |
|---|---:|---|
| `banco-frontend` | 5173 | Interfaz para clientes y empleados |
| `banco-bff` | 8080 | Entrada al backend, autorización y coordinación de servicios |
| `cuentas-service` | 8081 | Gestión y habilitación de cuentas |
| `solicitudes-service` | 8082 | Creación, aprobación y rechazo de solicitudes |
| `kafka-service` | 8083 | Consumo de eventos, métricas y administración de Kafka |

**RabbitMQ** maneja la mensajería operativa de solicitudes, los reintentos y los mensajes fallidos mediante una DLQ. Desde el panel de Empleado se pueden consultar y administrar colas, exchanges y bindings.

**Kafka** registra los eventos de solicitudes creadas y cuentas habilitadas. `kafka-service` consume ambos flujos y guarda las métricas en MySQL. El menú **Empleado → Kafka** permite consultar métricas y grupos de consumidores, crear y eliminar tópicos y cambiar su retención.

| Tópico | Retención |
|---|---:|
| `bancocloud.solicitudes.creadas.v1` | 7 días |
| `bancocloud.cuentas.habilitadas.v1` | 30 días |

Los tópicos tienen **3 particiones**. El entorno local usa **3 brokers con ZooKeeper** y Kafka UI. El despliegue en AWS usa **1 broker Kafka en modo KRaft**, con factor de replicación `1`.

## Ejecución local

**Requisitos:** Docker con Compose, Java 21, Node.js 22.12 o superior y una cuenta de Cognito.

### 1. Levanta la infraestructura

Desde la raíz, crea `.env` usando [`.env.example`](.env.example) como plantilla y define las credenciales locales de RabbitMQ. Luego ejecuta:

~~~powershell
docker compose --profile rabbitmq-cluster up -d --wait rabbitmq-cluster-1 rabbitmq-cluster-2 rabbitmq-cluster-3
docker compose --profile kafka up -d --wait --wait-timeout 240 kafka-ui
docker start bancocloud-mysql-local
~~~

Kafka UI está disponible en [localhost:8085](http://localhost:8085/).

<details>
<summary>Crear el contenedor MySQL en la primera ejecución</summary>

Crea `.env.mysql-local` en la raíz con estos valores. Elige tus propias contraseñas de prueba.

~~~dotenv
MYSQL_DATABASE=bancocloud_local
MYSQL_USER=usuario_local
MYSQL_PASSWORD=tu_clave_mysql
MYSQL_ROOT_PASSWORD=tu_clave_root
~~~

Ejecuta este comando una sola vez en lugar de `docker start`:

~~~powershell
docker run -d --name bancocloud-mysql-local -p 127.0.0.1:3307:3306 --env-file .env.mysql-local -v bancocloud-mysql-local-data:/var/lib/mysql mysql:8.4
~~~

</details>

### 2. Configura e inicia el backend

Crea `application-local.properties` en la raíz. Usa el usuario y la contraseña del contenedor MySQL y los valores de RabbitMQ de tu `.env`.

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

Abre una terminal PowerShell dentro de cada módulo Java y ejecuta las mismas dos líneas:

~~~powershell
$env:SPRING_CONFIG_ADDITIONAL_LOCATION = 'file:../application-local.properties'
.\mvnw.cmd spring-boot:run
~~~

Comprueba que `/actuator/health` devuelva `UP` en los puertos `8080`, `8081`, `8082` y `8083`.

### 3. Inicia el frontend

Desde la raíz del proyecto, en otra terminal PowerShell:

~~~powershell
Set-Location banco-frontend
$env:VITE_API_BASE_URL = 'http://localhost:8080'
npm.cmd ci
npm.cmd run dev -- --strictPort
~~~

Abre [BancoCloud local](http://localhost:5173/). Usa `npm.cmd run build` para generar el build de producción.

## Probar el flujo

1. Inicia sesión como **Cliente** y crea una solicitud de prueba.
2. Inicia sesión como **Empleado** y aprueba esa solicitud.
3. Ve a **Kafka → Métricas**. Deben aparecer un evento de **Solicitudes creadas** y otro de **Cuentas habilitadas**.
4. En **Grupos**, revisa los offsets y el lag. En **RabbitMQ**, revisa las colas, los exchanges y los bindings.

Si pruebas la creación de tópicos desde el panel, usa **3 réplicas en local** y **1 en AWS**.

Los dos flujos se verificaron de punta a punta en la aplicación publicada en Amplify. Las [pruebas de integración de Kafka](kafka-service/src/test/java/cl/duoc/kafka_service/KafkaServiceIntegrationTest.java) se ejecutan con `.\mvnw.cmd test` desde `kafka-service`.
