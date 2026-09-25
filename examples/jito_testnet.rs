use solana_low_latency::jito::client::JitoClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Создать JitoClient
    let jito_url = "https://testnet.block-engine.jito.wtf";
    let client = JitoClient::new(jito_url);

    println!("Отправка запроса к Jito Testnet Block Engine...");

    // TODO: Вызвать get_tip_accounts_raw().await
    let result = client.get_tip_accounts_raw().await?;

    // TODO: Красиво вывести ответ
    println!("Получен ответ от Jito:");
    println!("{}", serde_json::to_string_pretty(&result)?);

    let accounts = client.get_tip_accounts().await?;

    // Проверить и вывести результаты
    println!("Успешно распарсено аккаунтов: {}", accounts.len());
    println!("accounts.len() > 0: {}", accounts.len() > 0);
    
    if !accounts.is_empty() {
        println!("Первый полученный Pubkey для проверки: {}", accounts[0]);
    }

    Ok(())
}
