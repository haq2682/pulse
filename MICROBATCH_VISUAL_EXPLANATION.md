================================================================================
VISUAL EXPLANATION: Micro-Batch Data Integration Problem
================================================================================

YOUR QUESTION:
"If new data comes in micro-batches, will sums include old + new data?"

ANSWER: NO (currently) - Here's why:

================================================================================
SCENARIO: Customer C1 making multiple orders
================================================================================

┌─────────────────────────────────────────────────────────────────────────┐
│                         WHAT YOU EXPECT ✅                              │
└─────────────────────────────────────────────────────────────────────────┘

Time 10:00:00
┌──────────────────┐
│ Order: $100      │ ──┐
└──────────────────┘   │
                       ├─→ State: C1 = $100
                       │   Output: C1 total = $100 ✅
Time 10:00:10         │
┌──────────────────┐  │
│ Order: $200      │ ─┤
└──────────────────┘  │
                      └─→ State: C1 = $100 + $200 = $300
                          Output: C1 total = $300 ✅

Time 10:00:20
┌──────────────────┐
│ Order: $50       │ ──→ State: C1 = $300 + $50 = $350
└──────────────────┘     Output: C1 total = $350 ✅

RESULT: Dashboard shows C1 total = $350 (CORRECT - All 3 orders)


================================================================================

┌─────────────────────────────────────────────────────────────────────────┐
│                    WHAT ACTUALLY HAPPENS ❌                             │
└─────────────────────────────────────────────────────────────────────────┘

Time 10:00:00
┌──────────────────┐
│ Micro-Batch #1   │
│ Order: $100      │ ──→ Aggregate: SUM($100) = $100
└──────────────────┘     Output: C1 = $100
                         Dashboard: C1 = $100 ✅ (Correct so far)

Time 10:00:10
┌──────────────────┐
│ Micro-Batch #2   │
│ Order: $200      │ ──→ Aggregate: SUM($200) = $200  (Only this batch!)
└──────────────────┘     Output: C1 = $200
                         Dashboard: C1 = $200 ❌ (Should be $300)

Time 10:00:20
┌──────────────────┐
│ Micro-Batch #3   │
│ Order: $50       │ ──→ Aggregate: SUM($50) = $50   (Only this batch!)
└──────────────────┘     Output: C1 = $50
                         Dashboard: C1 = $50 ❌ (Should be $350)

RESULT: Dashboard shows C1 total = $50 (WRONG - Only last order)


================================================================================
WHY THIS HAPPENS
================================================================================

Current Code Flow:

┌─────────────────────────────────────────────────────────────────────────┐
│                      AGGREGATION FUNCTION                               │
│  def aggregate_customers(dataframes):                                   │
│      customer_agg = (                                                   │
│          dataframes["orders"]  ← Contains ONLY new orders from batch!  │
│          .groupBy("customer_id")                                        │
│          .agg(sum("total_amount"))  ← Sums only what it sees!         │
│      )                                                                  │
└─────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    STREAMING TRANSFORMATION                             │
│  def apply_batch_transformation(batch_df, batch_id):                   │
│      # batch_df = Only new records from last 10 seconds                │
│      dataframes = {"orders": batch_df}  ← Only partial data!          │
│      aggregate_customers(dataframes)     ← Function gets partial data! │
└─────────────────────────────────────────────────────────────────────────┘

PROBLEM: Function expects ALL data but receives ONLY new batch!


================================================================================
COMPARISON: Batch vs Batch vs Stateful Streaming
================================================================================

Approach 1: CURRENT (Micro-Batch Without State)
═════════════════════════════════════════════════
Batch 1: [Order $100]     → C1 = $100
Batch 2: [Order $200]     → C1 = $200  ❌ (Should be $300)
Batch 3: [Order $50]      → C1 = $50   ❌ (Should be $350)

Each batch processes independently, no memory of previous batches


Approach 2: FULL RE-AGGREGATION (Workaround)
═════════════════════════════════════════════════
Batch 1: Read ALL data [Order $100]              → C1 = $100  ✅
Batch 2: Read ALL data [Order $100, $200]        → C1 = $300  ✅
Batch 3: Read ALL data [Order $100, $200, $50]   → C1 = $350  ✅

Works but slow - re-reads and re-processes everything every 10 seconds!


Approach 3: STATEFUL STREAMING (Correct Solution)
═════════════════════════════════════════════════════
                        Spark Maintains State
                        ┌──────────────────┐
Batch 1: [Order $100] → │ C1: count=1      │ → Output: C1 = $100  ✅
                        │     sum=$100     │
                        └──────────────────┘
                               │
                               ▼ UPDATE
                        ┌──────────────────┐
Batch 2: [Order $200] → │ C1: count=2      │ → Output: C1 = $300  ✅
                        │     sum=$300     │
                        └──────────────────┘
                               │
                               ▼ UPDATE
                        ┌──────────────────┐
Batch 3: [Order $50]  → │ C1: count=3      │ → Output: C1 = $350  ✅
                        │     sum=$350     │
                        └──────────────────┘

Correct AND fast - only processes new records, state automatically maintained!


================================================================================
DATA FLOW DIAGRAM
================================================================================

Current Pipeline (PR #54):

┌──────────────┐  10s   ┌─────────┐  500ms  ┌──────────────┐
│   Database   │ ─────→ │  Kafka  │ ──────→ │ MinIO/mapped │
└──────────────┘ poll   └─────────┘ batches └──────────────┘
                                                     │
                                                     ▼ 10s batches
                                             ┌──────────────────┐
                                             │ Streaming Clean  │
                                             └──────────────────┘
                                                     │
                                                     ▼
                                             ┌──────────────────────┐
                                             │ MinIO/cleaned        │
                                             └──────────────────────┘
                                                     │
                                                     ▼ 10s batches
                                             ┌──────────────────────┐
                                             │ AGGREGATION          │
                                             │ ❌ Independent batch │
                                             │ ❌ No state          │
                                             │ ❌ No cumulative     │
                                             └──────────────────────┘
                                                     │
                                                     ▼
                                             ┌──────────────────────┐
                                             │ Dashboard            │
                                             │ Shows: Latest batch  │
                                             │ ❌ WRONG TOTALS!     │
                                             └──────────────────────┘


Needed Pipeline (Stateful Streaming):

┌──────────────┐  10s   ┌─────────┐  500ms  ┌──────────────┐
│   Database   │ ─────→ │  Kafka  │ ──────→ │ MinIO/mapped │
└──────────────┘ poll   └─────────┘ batches └──────────────┘
                                                     │
                                                     ▼ 10s batches
                                             ┌──────────────────┐
                                             │ Streaming Clean  │
                                             └──────────────────┘
                                                     │
                                                     ▼
                                             ┌──────────────────────┐
                                             │ MinIO/cleaned        │
                                             └──────────────────────┘
                                                     │
                                                     ▼ 10s batches
                  ┌──────────────────┐              │
                  │ Spark State      │◄─────────────┤
                  │ (Checkpoints)    │              │
                  │ - Customer: C1   │              ▼
                  │   Count: 3       │      ┌──────────────────────┐
                  │   Sum: $350      │      │ AGGREGATION          │
                  │ - Customer: C2   │◄────►│ ✅ Stateful          │
                  │   Count: 5       │      │ ✅ Cumulative        │
                  │   Sum: $1,200    │      │ ✅ Auto-update       │
                  └──────────────────┘      └──────────────────────┘
                                                     │
                                                     ▼
                                             ┌──────────────────────┐
                                             │ Dashboard            │
                                             │ Shows: All-time      │
                                             │ ✅ CORRECT TOTALS!   │
                                             └──────────────────────┘


================================================================================
SUMMARY
================================================================================

YOUR QUESTION:
"Will new data automatically integrate with old data in aggregations?"

CURRENT ANSWER: ❌ NO
- Each micro-batch processed independently
- No state maintained between batches
- Aggregations show only latest batch data

NEEDED SOLUTION: ✅ Stateful Streaming
- Spark maintains cumulative state
- New data automatically merges with existing totals
- Aggregations show correct cumulative metrics

TIMELINE: 2-4 weeks to implement

PRIORITY: HIGH - Blocking for production (dashboards show wrong numbers)

================================================================================

Read full documentation:
- README_MICROBATCH_QUESTION.md (quick start)
- ANSWER_YOUR_QUESTION_HERE.md (plain English)
- MICROBATCH_INTEGRATION_QUICK_ANSWER.md (technical)
- MICROBATCH_DATA_INTEGRATION_EXPLAINED.md (complete guide)

================================================================================
