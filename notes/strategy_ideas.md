# FOMO Continuation Pattern Detection
Here, the main strategies will be documented. For this strategy, we'll be using at least two constraints, an example may look like if x > t_1 and y < t_2, where x and y are variables and t_1 and t_2 are preset thresholds.

## Core Hypothesis
Morning momentum stocks that exhibit specific characteristics tend to continue rallying throughout the day, driven by FOMO (Fear of Missing Out) behaviour. Our goal is to identify them. This document captures all the key ideas from your research in a structured, actionable format. Organized as a reference and document for implementation. These are ideas, with values as placeholders.

## Constraint Variables
- **p_surge**: Price gain % from open to 10:00 AM
- **v_rel**: Relative volume vs 10-day average
- **r_close**: Distance from close to daily high (%)
- **f_size**: Float size (millions)
- **m_cap**: Market cap (billions)

### Example Constraint Combinations to Test
1. **Basic FOMO**: p_surge > 5% AND v_rel > 2 AND v_rel < 10
2. **Low Float Runner**: p_surge > 3% AND f_size < 20M AND r_close < 5%
3. **Clean Momentum**: p_surge > 5% AND no_retrace_below_open = True AND m_cap < 2B

## Filtering Process
### Stage 1: 
- Gap up >3% or early morning surge >5%
- Volume >1M shares in first 30 minutes
- Price >$1 (avoid penny stocks)
- Not more than 2x volume spike compared to average of last 10 days (filter out pump-and-dumps)
  
### Stage 2: Momentum Confirmation
- Maintaining gains through 10:00 AM
- No major retracement (<50% of morning gains)

### Stock Characteristics
- **Market Cap:** Focus on small/mid-cap (<$2-3B) for maximum volatility
- **Float:** Prefer low float stocks (<50M shares, ideally <20M)
- **Price Range:** Often <$20 stocks (more accessible to retail FOMO)
- **Sector Bias:** Tech, biotech, and meme sectors show highest FOMO potential

## Primary Detection Criteria
### 1. Price Action Signals

- **Morning Surge:** +5% gain from open by 10:00 AM (test variations: 3%, 5%, 10%)
- **Strength Indicator:** Close within 5% of daily high (eliminates "pop and drop" patterns)
- **Clean Uptrend:** No retracement below opening price after initial surge
- **Technical Context:** Breaking above key resistance, 50/200-day MA, or making new highs

## Backtesting Ideas

### 1. Optimize Thresholds:

- **Test morning surge:** 3%, 5%, 7%, 10%
- **Test volume multiples:** 1.5x, 2x, 3x, 5x
- **Test float limits:** <20M, <50M, <100M

### 2. Time Windows:

- **Entry:** 9:45, 10:00, 10:15, 10:30
- **Exit:** 3:30, 3:45, EOD

### 3. Combination Rules:

- Surge + Volume
- Surge + Volume + Float
- Surge + Volume + Technical

## Final Selection (Target of around 10 stocks)
- Low float or high short interest
- Clean technical setup (breakout, no overhead resistance)
- Potential catalyst presence (news, earnings, FDA, etc.)

## Risk Filters (Avoid False Positives)
- **Spread Filter**: Bid-ask spread < 2% (avoid illiquid names)
- **Pump Protection**: v_rel < 10x (extreme spikes often reverse)
- **News Check**: Verify legitimate catalyst if v_rel > 5x

## Implementation Notes
- Calculate all metrics at 10:00 AM for consistency
- Use 10-day rolling average for volume baseline
- Exclude stocks with price < $1 or volume < 100K shares
- Run scanner at 10:05 AM to allow for data lag
