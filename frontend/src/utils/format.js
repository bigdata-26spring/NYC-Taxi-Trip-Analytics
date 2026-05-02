export function compact(value) {
  const number = Number(value || 0);

  if (Math.abs(number) >= 1_000_000_000) {
    return `${(number / 1_000_000_000).toFixed(1)}B`;
  }
  if (Math.abs(number) >= 1_000_000) {
    return `${(number / 1_000_000).toFixed(1)}M`;
  }
  if (Math.abs(number) >= 1_000) {
    return `${(number / 1_000).toFixed(1)}K`;
  }

  return number.toLocaleString("en-US", { maximumFractionDigits: 0 });
}

export function integer(value) {
  return Number(value || 0).toLocaleString("en-US", { maximumFractionDigits: 0 });
}

export function money(value) {
  const number = Number(value || 0);

  if (Math.abs(number) >= 1_000_000_000) {
    return `$${(number / 1_000_000_000).toFixed(2)}B`;
  }
  if (Math.abs(number) >= 1_000_000) {
    return `$${(number / 1_000_000).toFixed(1)}M`;
  }

  return number.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  });
}

export function decimal(value, digits = 1) {
  return Number(value || 0).toLocaleString("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

export function pct(value, digits = 1) {
  return `${(Number(value || 0) * 100).toFixed(digits)}%`;
}

export function labelize(value) {
  return String(value || "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}
