#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComputeBudget {
    compute_unit_limit: u32,
    compute_unit_price_micro_lamports: u64,
}

impl ComputeBudget {
    pub fn new(compute_unit_limit: u32, compute_unit_price_micro_lamports: u64) -> Self {
        Self {
            compute_unit_limit,
            compute_unit_price_micro_lamports,
        }
    }

    pub fn priority_fee_lamports(
        compute_unit_limit: u32,
        compute_unit_price_micro_lamports: u64,
    ) -> u128 {
        (compute_unit_limit as u128 * compute_unit_price_micro_lamports as u128).div_ceil(1_000_000)
    }

    pub fn with_margin(consumed: u32, margin_percent: u32, max_limit: u32) -> u32 {
        let res = consumed as u64 + (consumed as u64 * margin_percent as u64).div_ceil(100);

        if res > max_limit as u64 {
            return max_limit;
        }
        res as u32
    }

    pub fn to_instructions(&self) -> [solana_client::rpc_response::transaction::Instruction; 2] {
        [
            solana_compute_budget_interface::ComputeBudgetInstruction::set_compute_unit_limit(
                self.compute_unit_limit,
            ),
            solana_compute_budget_interface::ComputeBudgetInstruction::set_compute_unit_price(
                self.compute_unit_price_micro_lamports,
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use crate::compute_budget::ComputeBudget;

    #[test]
    fn with_margin_supports_other_percentages() {
        let limit = ComputeBudget::with_margin(100_000, 20, 1_400_000);

        assert_eq!(limit, 120_000);
    }

    #[test]
    fn with_margin_adds_normal_margin() {
        let limit = ComputeBudget::with_margin(100_000, 10, 1_400_000);

        assert_eq!(limit, 110_000);
    }

    #[test]
    fn with_margin_zero_consumed_stays_zero() {
        let limit = ComputeBudget::with_margin(0, 10, 1_400_000);

        assert_eq!(limit, 0);
    }

    #[test]
    fn with_margin_does_not_exceed_max_limit() {
        let limit = ComputeBudget::with_margin(1_390_000, 10, 1_400_000);

        assert_eq!(limit, 1_400_000);
    }

    #[test]
    fn with_margin_handles_large_values_without_overflow() {
        let limit = ComputeBudget::with_margin(u32::MAX, 100, 1_400_000);

        assert_eq!(limit, 1_400_000);
    }

    #[test]
    fn multiplication_can_overflow_before_max() {
        let limit = ComputeBudget::with_margin(3_000_000_000, 100, u32::MAX);

        assert_eq!(limit, u32::MAX);
    }

    #[test]
    fn with_margin_rounds_up() {
        let limit = ComputeBudget::with_margin(1, 10, 1_400_000);

        assert_eq!(limit, 2);
    }

    #[test]
    fn zero_price_means_zero_priority_fee() {
        let fee = ComputeBudget::priority_fee_lamports(100_000, 0);
        assert_eq!(fee, 0);

        let fee_large_limit = ComputeBudget::priority_fee_lamports(1_400_000, 0);
        assert_eq!(fee_large_limit, 0);
    }

    #[test]
    fn computes_exact_priority_fee() {
        let fee = ComputeBudget::priority_fee_lamports(200_000, 5_000_000);
        assert_eq!(fee, 1_000_000);
    }

    #[test]
    fn rounds_priority_fee_up() {
        let fee = ComputeBudget::priority_fee_lamports(1, 1);
        assert_eq!(fee, 1);

        let fee_exact_plus_one = ComputeBudget::priority_fee_lamports(1_000_001, 1);
        assert_eq!(fee_exact_plus_one, 2);
    }

    #[test]
    fn larger_requested_limit_costs_more() {
        // должен стоить строго больше (или как минимум не меньше из-за округления).
        let price = 2_500;
        let low_limit_fee = ComputeBudget::priority_fee_lamports(200_000, price);
        let high_limit_fee = ComputeBudget::priority_fee_lamports(400_000, price);

        assert!(
            high_limit_fee > low_limit_fee,
            "Больший лимит должен стоить дороже. Малый: {}, Большой: {}",
            low_limit_fee,
            high_limit_fee
        );
    }

    #[test]
    fn test_to_instructions() {
        // 1. Инициализируем нашу структуру тестовыми данными
        let compute_unit_limit = 400_000;
        let compute_unit_price_micro_lamports = 10_000;

        let budget = ComputeBudget::new(compute_unit_limit, compute_unit_price_micro_lamports);

        // 2. Получаем результат работы тестируемого метода
        let actual = budget.to_instructions();

        // 3. Создаем ожидаемые инструкции с помощью официального SDK
        let expected_limit_instruction = solana_compute_budget_interface::ComputeBudgetInstruction::set_compute_unit_limit(compute_unit_limit);
        let expected_price_instruction = solana_compute_budget_interface::ComputeBudgetInstruction::set_compute_unit_price(compute_unit_price_micro_lamports);

        // 4. Проверяем, что полученные инструкции полностью совпадают с ожидаемыми
        assert_eq!(
            actual[0], expected_limit_instruction,
            "Первая инструкция должна устанавливать лимит кубов (ComputeUnitLimit)"
        );
        assert_eq!(
            actual[1], expected_price_instruction,
            "Вторая инструкция должна устанавливать цену куба (ComputeUnitPrice)"
        );
    }
}
