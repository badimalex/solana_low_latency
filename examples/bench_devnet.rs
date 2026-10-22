use std::time::{Duration, SystemTime, UNIX_EPOCH};

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_keypair::Keypair;
use solana_keypair::{Signer, read_keypair_file};
use solana_low_latency::jito::client::JitoClient;
use solana_message::Instruction;
use solana_pubkey::Pubkey;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use solana_commitment_config::CommitmentConfig;
use solana_system_interface::instruction::transfer;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Semaphore;
use tokio::task::LocalSet;

use solana_low_latency::{
    blockhash::RpcBlockhashSource,
    confirmation::{ConfirmationTracker, RpcStatusProvider},
    execution::{
        ExecutionEngine,
        retry::RetryPolicy,
        sender::SolanaRpcSender,
        types::{ExecutionError, ExecutionRequest, ExecutionRoute},
    },
};

#[derive(Debug, serde::Serialize)]
pub struct OperationRecord {
    pub operation_index: usize,
    pub timestamp: u128,
    pub route: String,
    pub signature: Option<String>,
    pub confirmation_status: Option<String>,
    pub latency_ms: f64,
    pub error: Option<String>,
}

fn make_request(payer: &Keypair, recipient: &Pubkey, index: usize) -> ExecutionRequest {
    // TODO 1: Создаем уникальную инструкцию трансфера
    // Используем `index` для динамического изменения суммы (lamports),
    // чтобы гарантировать уникальность сообщения и избежать дублирования подписей.
    let lamports = 1000 + (index as u64);

    let transfer_instruction = transfer(&payer.pubkey(), recipient, lamports);

    let instructions: Vec<Instruction> = vec![transfer_instruction];

    ExecutionRequest {
        instructions,
        payer: payer.insecure_clone(),
        route: ExecutionRoute::SolanaRpc,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let payer = read_keypair_file("devnet-payer.json")?;
    let payer = std::rc::Rc::new(read_keypair_file("devnet-payer.json")?);

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // .with_max_level(tracing::Level::WARN)
        .init();

    const TOTAL_OPERATIONS: usize = 20;
    const CONCURRENCY: usize = 1;

    // Официальный публичный RPC эндпоинт для Solana Devnet
    let rpc_url = "https://api.devnet.solana.com".to_string();

    // let blockhash_info = fetch_latest(&rpc_client, commitment).await?;

    let blockhash_source = RpcBlockhashSource::new(
        RpcClient::new(rpc_url.clone()),
        CommitmentConfig::confirmed(),
    );

    let recipient: Pubkey = "AMncHssJAJAk65mudELEbuHH4ADn7H6DkGJgEQ3NU5D1".parse()?;
    let rpc_sender = SolanaRpcSender::new(RpcClient::new(rpc_url.clone()));

    let confirmation_tracker =
        ConfirmationTracker::new(RpcStatusProvider::new(RpcClient::new(rpc_url)));

    let jito_sender = JitoClient::new("https://testnet.block-engine.jito.wtf");

    let engine = Rc::new(ExecutionEngine::new(
        blockhash_source,
        rpc_sender,
        jito_sender,
        confirmation_tracker,
        Duration::from_secs(40),
        RetryPolicy::new(1, Duration::from_millis(100), Duration::from_millis(100)).unwrap(),
        CancellationToken::new(),
        Duration::from_secs(60),
    ));

    // Один общий ExecutionEngine
    // let engine = Rc::new(make_engine());

    // Ограничение concurrency
    let semaphore = Rc::new(Semaphore::new(CONCURRENCY));

    // Сколько операций выполняется сейчас
    let active = Rc::new(AtomicUsize::new(0));

    // Максимальное одновременное количество
    let max_active = Rc::new(AtomicUsize::new(0));

    // Для запуска локальных async tasks без Send
    let local = LocalSet::new();

    let total_start = Instant::now();

    let (mut latencies, success_count, failure_count, unknown_count, records) = local
        .run_until(async {
            let mut handles = Vec::with_capacity(TOTAL_OPERATIONS);

            for ind in 0..TOTAL_OPERATIONS {
                let engine = Rc::clone(&engine);
                let semaphore = Rc::clone(&semaphore);
                let active = Rc::clone(&active);
                let max_active = Rc::clone(&max_active);
                let payer = Rc::clone(&payer);

                let handle = tokio::task::spawn_local(async move {
                    // TODO 2: Получить permit из Semaphore.
                    // Метод acquire_owned() забирает permit и связывает его с лайфтаймом переменной.
                    let _permit = semaphore.acquire().await.unwrap();

                    // TODO 3: Увеличить active и обновить max_active.
                    let current_active = active.fetch_add(1, Ordering::SeqCst) + 1;

                    // Атомарно обновляем максимум (CAS-петля)
                    let mut prev = max_active.load(Ordering::SeqCst);
                    while current_active > prev {
                        match max_active.compare_exchange_weak(
                            prev,
                            current_active,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(_) => break,
                            Err(actual) => prev = actual,
                        }
                    }

                    // TODO 4: Создать ExecutionRequest и измерить latency.
                    let request = make_request(&payer, &recipient, ind);

                    let start = Instant::now();
                    let started_unix_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis();
                    let result = engine.execute(request).await;
                    let duration = start.elapsed();
                    let latency_ms = duration.as_secs_f64() * 1000.0;

                    active.fetch_sub(1, Ordering::SeqCst);

                    (result, latency_ms, started_unix_ms)
                });

                handles.push(handle);
            }

            // ======================================
            // TODO 6 — Собрать результаты
            // ======================================

            let mut latencies: Vec<f64> = Vec::new();
            let mut success_count = 0usize;
            let mut failure_count = 0usize;
            let mut unknown_count = 0usize;

            let mut records: Vec<OperationRecord> = Vec::new();

            for (operation_index, handle) in handles.into_iter().enumerate() {
                // Дожидаемся завершения таски
                let (result, latency_ms, started_unix_ms) = handle.await.unwrap();
                latencies.push(latency_ms);

                let route = "SolanaRpc".to_string();

                let mut record = OperationRecord {
                    operation_index,
                    timestamp: started_unix_ms,
                    route,
                    signature: None,
                    confirmation_status: None,
                    latency_ms,
                    error: None,
                };

                // Классифицируем ошибки
                match result {
                    Ok(outcome) => {
                        success_count += 1;
                        record.signature = Some(outcome.signature.to_string());
                        record.confirmation_status = Some(format!("{:?}", outcome.confirmation));
                    }
                    Err(ExecutionError::OutcomeUnknown { signature }) => {
                        unknown_count += 1;
                        record.signature = Some(signature.to_string());
                        record.error = Some("OutcomeUnknown".to_string());
                    }
                    Err(ExecutionError::ConfirmationTimeout) => {
                        unknown_count += 1;
                        record.error = Some("ConfirmationTimeout".to_string());
                    }

                    Err(ExecutionError::ConfirmationFailed) => {
                        unknown_count += 1;
                        record.error = Some("ConfirmationFailed".to_string());
                    }
                    Err(other_error) => {
                        failure_count += 1;
                        record.error = Some(format!("{:?}", other_error));
                    }
                }
                records.push(record);
            }

            (
                latencies,
                success_count,
                failure_count,
                unknown_count,
                records,
            )
        })
        .await;

    let total_duration = total_start.elapsed();
    let total_seconds = total_duration.as_secs_f64();

    // std::fs::write("devnet_100.json", serde_json::to_vec_pretty(&records)?)?;
    std::fs::write("devnet_diag_20_v2.json", serde_json::to_vec_pretty(&records)?)?;

    // ======================================
    // TODO 7 — Throughput
    // ======================================

    let completed_operations = success_count + failure_count + unknown_count;
    let throughput = if total_seconds > 0.0 {
        completed_operations as f64 / total_seconds
    } else {
        0.0
    };

    println!("D13 CONCURRENT BENCHMARK");
    println!("Operations: {}", TOTAL_OPERATIONS);
    println!("Concurrency limit: {}", CONCURRENCY);
    println!("Max active: {}", max_active.load(Ordering::Relaxed));
    println!("Success: {}", success_count);
    println!("Failure: {}", failure_count);
    println!("Unknown: {}", unknown_count);
    println!("Latencies collected: {}", latencies.len());
    println!("Latencies collected: {:?}", latencies);
    println!(
        "Total duration: {:.2} ms",
        total_duration.as_secs_f64() * 1000.0
    );
    println!("Throughput: {:.2} ops/sec", throughput);

    // После сбора всех результатов:

    latencies.sort_by(|a, b| a.total_cmp(b));

    // TODO 3:
    // Вычислить p50, p95 и p99.

    println!("p50: {:?} ms", percentile(&latencies, 0.50));
    println!("p95: {:?} ms", percentile(&latencies, 0.95));
    println!("p99: {:?} ms", percentile(&latencies, 0.99));

    Ok(())
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if p == 0.0 {
        return sorted[0];
    }
    // TODO 1:
    // Вычислить индекс по методу nearest rank.
    // p передаётся в диапазоне 0.0..=1.0.

    // TODO 2:
    // Вернуть соответствующую latency.
    let ind = (sorted.len() as f64 * p).ceil() - 1.0;
    sorted[ind as usize]
}

#[test]
fn test_percentile_min_and_max() {
    let data = vec![10.0, 20.0, 30.0, 40.0, 50.0];

    // 0-й перцентиль (минимум) должен возвращать первый элемент
    assert_eq!(percentile(&data, 0.0), 10.0);

    // 100-й перцентиль (максимум) должен возвращать последний элемент
    assert_eq!(percentile(&data, 1.0), 50.0);
}
