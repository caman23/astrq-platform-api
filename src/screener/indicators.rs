//! Technical indicators on daily bars. Every function returns one value per bar (aligned
//! with the input), `None` until enough history exists. Standard definitions:
//! SMA; EMA seeded with the SMA of the first `n` values (α = 2/(n+1)); RSI and ATR with
//! Wilder smoothing; MACD = EMA(fast) − EMA(slow), signal = EMA(signal) of MACD.

pub type Series = Vec<Option<f64>>;

pub fn sma(x: &[f64], n: usize) -> Series {
    let mut out = vec![None; x.len()];
    if n == 0 {
        return out;
    }
    let mut sum = 0.0;
    for i in 0..x.len() {
        sum += x[i];
        if i >= n {
            sum -= x[i - n];
        }
        if i + 1 >= n {
            out[i] = Some(sum / n as f64);
        }
    }
    out
}

pub fn ema(x: &[f64], n: usize) -> Series {
    let mut out = vec![None; x.len()];
    if n == 0 || x.len() < n {
        return out;
    }
    let a = 2.0 / (n as f64 + 1.0);
    let mut e = x[..n].iter().sum::<f64>() / n as f64;
    out[n - 1] = Some(e);
    for i in n..x.len() {
        e = a * x[i] + (1.0 - a) * e;
        out[i] = Some(e);
    }
    out
}

/// EMA over a series that starts with `None`s (e.g. MACD line).
fn ema_opt(x: &Series, n: usize) -> Series {
    let start = x.iter().position(Option::is_some).unwrap_or(x.len());
    let vals: Vec<f64> = x[start..].iter().map(|v| v.unwrap_or(0.0)).collect();
    let mut out = vec![None; start];
    out.extend(ema(&vals, n));
    out
}

pub fn rsi(close: &[f64], n: usize) -> Series {
    let mut out = vec![None; close.len()];
    if n == 0 || close.len() <= n {
        return out;
    }
    let (mut gain, mut loss) = (0.0, 0.0);
    for i in 1..=n {
        let d = close[i] - close[i - 1];
        gain += d.max(0.0);
        loss += (-d).max(0.0);
    }
    gain /= n as f64;
    loss /= n as f64;
    let val = |g: f64, l: f64| {
        if l == 0.0 {
            if g == 0.0 {
                50.0
            } else {
                100.0
            }
        } else {
            100.0 - 100.0 / (1.0 + g / l)
        }
    };
    out[n] = Some(val(gain, loss));
    for i in n + 1..close.len() {
        let d = close[i] - close[i - 1];
        gain = (gain * (n as f64 - 1.0) + d.max(0.0)) / n as f64;
        loss = (loss * (n as f64 - 1.0) + (-d).max(0.0)) / n as f64;
        out[i] = Some(val(gain, loss));
    }
    out
}

pub struct Macd {
    pub line: Series,
    pub signal: Series,
    pub hist: Series,
}

pub fn macd(close: &[f64], fast: usize, slow: usize, signal: usize) -> Macd {
    let (f, s) = (ema(close, fast), ema(close, slow));
    let line: Series = f.iter().zip(&s).map(|(a, b)| Some((*a)? - (*b)?)).collect();
    let sig = ema_opt(&line, signal);
    let hist = line
        .iter()
        .zip(&sig)
        .map(|(a, b)| Some((*a)? - (*b)?))
        .collect();
    Macd {
        line,
        signal: sig,
        hist,
    }
}

pub struct Bands {
    pub upper: Series,
    pub lower: Series,
    /// %B: 0 = lower band, 1 = upper band
    pub pct_b: Series,
}

pub fn bollinger(close: &[f64], n: usize, k: f64) -> Bands {
    let mid = sma(close, n);
    let mut upper = vec![None; close.len()];
    let mut lower = vec![None; close.len()];
    let mut pct_b = vec![None; close.len()];
    for i in 0..close.len() {
        let Some(m) = mid[i] else { continue };
        let w = &close[i + 1 - n..=i];
        let sd = (w.iter().map(|x| (x - m).powi(2)).sum::<f64>() / n as f64).sqrt();
        let (u, l) = (m + k * sd, m - k * sd);
        upper[i] = Some(u);
        lower[i] = Some(l);
        pct_b[i] = Some(if u > l { (close[i] - l) / (u - l) } else { 0.5 });
    }
    Bands {
        upper,
        lower,
        pct_b,
    }
}

/// Average true range as % of the close (comparable across price levels).
pub fn atr_pct(high: &[f64], low: &[f64], close: &[f64], n: usize) -> Series {
    let len = close.len();
    let mut out = vec![None; len];
    if n == 0 || len <= n {
        return out;
    }
    let tr = |i: usize| {
        let pc = close[i - 1];
        (high[i] - low[i])
            .max((high[i] - pc).abs())
            .max((low[i] - pc).abs())
    };
    let mut atr = (1..=n).map(tr).sum::<f64>() / n as f64;
    out[n] = Some(atr / close[n] * 100.0);
    for i in n + 1..len {
        atr = (atr * (n as f64 - 1.0) + tr(i)) / n as f64;
        out[i] = Some(atr / close[i] * 100.0);
    }
    out
}

/// % change over `n` bars.
pub fn roc(close: &[f64], n: usize) -> Series {
    (0..close.len())
        .map(|i| (i >= n && close[i - n] > 0.0).then(|| (close[i] / close[i - n] - 1.0) * 100.0))
        .collect()
}

/// Volume relative to its average of the previous `n` bars (today excluded).
pub fn volume_ratio(volume: &[f64], n: usize) -> Series {
    let avg = sma(volume, n);
    (0..volume.len())
        .map(|i| {
            let a = avg.get(i.checked_sub(1)?)?.as_ref().copied()?;
            (a > 0.0).then(|| volume[i] / a)
        })
        .collect()
}

/// Distance of the close from the highest high of the last `n` bars, in % (≤ 0).
pub fn from_high(high: &[f64], close: &[f64], n: usize) -> Series {
    (0..close.len())
        .map(|i| {
            (i + 1 >= n && n > 0).then(|| {
                let h = high[i + 1 - n..=i].iter().copied().fold(f64::MIN, f64::max);
                (close[i] / h - 1.0) * 100.0
            })
        })
        .collect()
}

/// Distance of the close above the lowest low of the last `n` bars, in % (≥ 0).
pub fn from_low(low: &[f64], close: &[f64], n: usize) -> Series {
    (0..close.len())
        .map(|i| {
            (i + 1 >= n && n > 0).then(|| {
                let l = low[i + 1 - n..=i].iter().copied().fold(f64::MAX, f64::min);
                (close[i] / l - 1.0) * 100.0
            })
        })
        .collect()
}

/// Stochastic %K (fast) over `n` bars.
pub fn stoch_k(high: &[f64], low: &[f64], close: &[f64], n: usize) -> Series {
    (0..close.len())
        .map(|i| {
            (i + 1 >= n && n > 0).then(|| {
                let h = high[i + 1 - n..=i].iter().copied().fold(f64::MIN, f64::max);
                let l = low[i + 1 - n..=i].iter().copied().fold(f64::MAX, f64::min);
                if h > l {
                    (close[i] - l) / (h - l) * 100.0
                } else {
                    50.0
                }
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(v: &Series) -> f64 {
        v.last().copied().flatten().unwrap()
    }

    #[test]
    fn sma_and_ema() {
        let x = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(
            sma(&x, 3),
            vec![None, None, Some(2.0), Some(3.0), Some(4.0)]
        );
        let e = ema(&x, 3);
        assert_eq!(e[2], Some(2.0));
        assert!((e[3].unwrap() - 3.0).abs() < 1e-12); // 0.5*4 + 0.5*2
        assert!((e[4].unwrap() - 4.0).abs() < 1e-12);
    }

    /// Wilder's RSI on the classic 14-period example (Wilder, 1978 / StockCharts table).
    #[test]
    fn rsi_matches_reference() {
        let c = [
            44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.10, 45.42, 45.84, 46.08, 45.89, 46.03,
            45.61, 46.28, 46.28, 46.00, 46.03, 46.41, 46.22, 45.64,
        ];
        let r = rsi(&c, 14);
        assert!(r[13].is_none());
        assert!((r[14].unwrap() - 70.46).abs() < 0.05, "{:?}", r[14]);
        assert!((r[15].unwrap() - 66.25).abs() < 0.05, "{:?}", r[15]);
        assert!((close(&r) - 57.92).abs() < 0.3, "{:?}", r.last());
    }

    #[test]
    fn macd_bands_and_ranges() {
        let c: Vec<f64> = (0..80)
            .map(|i| 100.0 + (i as f64 * 0.3).sin() * 5.0 + i as f64 * 0.2)
            .collect();
        let m = macd(&c, 12, 26, 9);
        assert!(m.line[24].is_none() && m.line[25].is_some());
        assert!(m.signal[32].is_none() && m.signal[33].is_some());
        let last = c.len() - 1;
        assert!(
            (m.hist[last].unwrap() - (m.line[last].unwrap() - m.signal[last].unwrap())).abs()
                < 1e-9
        );
        let b = bollinger(&c, 20, 2.0);
        assert!(b.upper[last].unwrap() > b.lower[last].unwrap());
        let h: Vec<f64> = c.iter().map(|x| x + 1.0).collect();
        let l: Vec<f64> = c.iter().map(|x| x - 1.0).collect();
        assert!(close(&from_high(&h, &c, 20)) <= 0.0);
        assert!(close(&from_low(&l, &c, 20)) >= 0.0);
        let k = close(&stoch_k(&h, &l, &c, 14));
        assert!((0.0..=100.0).contains(&k));
        assert!(close(&atr_pct(&h, &l, &c, 14)) > 0.0);
        assert!((close(&roc(&c, 1)) - (c[last] / c[last - 1] - 1.0) * 100.0).abs() < 1e-9);
        let v = [100.0, 100.0, 100.0, 300.0];
        assert_eq!(volume_ratio(&v, 3)[3], Some(3.0));
    }
}
