use rdkafka::producer::{FutureProducer, FutureRecord};
use std::{env, io};
use actix_web::{App, HttpServer, Responder, web};
use rdkafka::config::ClientConfig;

fn create_kafka_client_config() -> ClientConfig {
    let bootstrap_servers = env::var("KAFKA_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    //let api_version = env::var("KAFKA_API_VERSION").unwrap_or_else(|_| "auto".to_string());
    let security_protocol = env::var("KAFKA_SECURITY_PROTOCOL")
        .unwrap_or_else(|_| "PLAINTEXT".to_string());
    let retry_backoff_ms = env::var("KAFKA_RETRY_BACKOFF_MS")
        .unwrap_or_else(|_| "100".to_string())
        .parse::<u64>()
        .expect("Invalid value for retry_backoff_ms");
    let metadata_max_age_ms = env::var("KAFKA_METADATA_MAX_AGE_MS")
        .unwrap_or_else(|_| "300000".to_string())
        .parse::<u64>()
        .expect("Invalid value for metadata_max_age_ms");
    let request_timeout_ms = env::var("KAFKA_REQUEST_TIMEOUT_MS")
        .unwrap_or_else(|_| "40000".to_string())
        .parse::<u64>()
        .expect("Invalid value for request_timeout_ms");
    let connections_max_idle_ms = env::var("KAFKA_CONNECTIONS_MAX_IDLE_MS")
        .unwrap_or_else(|_| "540000".to_string())
        .parse::<u64>()
        .expect("Invalid value for connections_max_idle_ms");
    let acks = env::var("KAFKA_ACKS").unwrap_or_else(|_| "1".to_string());
    let enable_idempotence = env::var("KAFKA_ENABLE_IDEMPOTENCE")
        .unwrap_or_else(|_| "0".to_string())
        .parse::<i32>()
        .expect("Invalid value for enable_idempotence");
    let transactional_id = env::var("KAFKA_TRANSACTIONAL_ID").ok();
    let client_id = env::var("KAFKA_CLIENT_ID_PRODUCER").ok();
    //let key_serializer = env::var("KAFKA_KEY_SERIALIZER").ok(); // None if not set
    //let value_serializer = env::var("KAFKA_VALUE_SERIALIZER").ok(); // None if not set
    let compression_type = env::var("KAFKA_COMPRESSION_TYPE")
        .unwrap_or_else(|_| "none".to_string());
    let max_batch_size = env::var("KAFKA_MAX_BATCH_SIZE")
        .unwrap_or_else(|_| "16384".to_string())
        .parse::<u64>()
        .expect("Invalid value for max_batch_size");
    let max_request_size = env::var("KAFKA_MAX_REQUEST_SIZE")
        .unwrap_or_else(|_| "504857600".to_string())
        .parse::<u64>()
        .expect("Invalid value for max_request_size");
    let linger_ms = env::var("KAFKA_LINGER_MS")
        .unwrap_or_else(|_| "0".to_string())
        .parse::<u64>()
        .expect("Invalid value for linger_ms");
    let send_backoff_ms = env::var("KAFKA_SEND_BACKOFF_MS")
        .unwrap_or_else(|_| "100".to_string())
        .parse::<u64>()
        .expect("Invalid value for send_backoff_ms");
    let transaction_timeout_ms = env::var("KAFKA_TRANSACTION_TIMEOUT_MS")
        .unwrap_or_else(|_| "60000".to_string())
        .parse::<u64>()
        .expect("Invalid value for transaction_timeout_ms");
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &bootstrap_servers)
        .set("api.version.request", "true")
        .set("security.protocol", &security_protocol)
        .set("retry.backoff.ms", &retry_backoff_ms.to_string())
        .set("metadata.max.age.ms", &metadata_max_age_ms.to_string())
        .set("request.timeout.ms", &request_timeout_ms.to_string())
        .set("connections.max.idle.ms", &connections_max_idle_ms.to_string())
        .set("acks", &acks)
        .set("enable.idempotence", &enable_idempotence.to_string())
        .set("transactional.id", &transactional_id.unwrap_or_else(|| "".to_string()))
        .set("client.id", &client_id.unwrap_or_else(|| "".to_string()))
        //.set("key.serializer", &key_serializer.unwrap_or_else(|| "".to_string()))
        //.set("value.serializer", &value_serializer.unwrap_or_else(|| "".to_string()))
        .set("compression.type", &compression_type)
        .set("batch.size", &max_batch_size.to_string())
        .set("message.max.bytes", &max_request_size.to_string())
        .set("linger.ms", &linger_ms.to_string())
        .set("retry.backoff.ms", &send_backoff_ms.to_string())
        .set("transaction.timeout.ms", &transaction_timeout_ms.to_string());
    config
}

fn create_kafka_producer() -> FutureProducer {
    let producer_config = create_kafka_client_config();
    producer_config.create().expect("Failed to create Kafka producer")
}

async fn send_to_kafka(
    producer: web::Data<FutureProducer>,
    payload: web::Json<serde_json::Value>,
) -> impl Responder {
    let message = payload.to_string();
    let record: FutureRecord<str, String> = FutureRecord::to("before-processor-topic").payload(&message);

    match producer.send(record, rdkafka::util::Timeout::Never).await {
        Ok(_) => {
            "Message sent to Kafka successfully".to_string()
        },
        Err(err) => {
            format!("Error sending message to Kafka: {:?}", err)
        }
    }
}

#[actix_web::main]
async fn main() -> io::Result<()> {
    let port = env::var("PORT").unwrap_or_else(|_| "5666".to_string());
    let kafka_producer = web::Data::new(create_kafka_producer());
    HttpServer::new(move || {
        App::new()
            .app_data(kafka_producer.clone())
            .route("/", web::post().to(send_to_kafka))
    })
        .bind(format!("0.0.0.0:{}", port))?
        .run()
        .await
}