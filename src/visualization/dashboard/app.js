const FILES = {
  kpi: "kpi_summary.csv",
  hourly: "hourly_demand.csv",
  daily: "daily_demand.csv",
  monthly: "monthly_demand.csv",
  weekdayWeekend: "weekday_weekend_hourly.csv",
  heatmap: "weekday_hour_heatmap.csv",
  borough: "borough_hourly_pattern.csv",
  rush: "rush_hour_summary.csv",
  zones: "top_zones_overall.csv",
  routes: "top_routes.csv",
  forecast: "forecast_demand.csv",
};

const NUMERIC_COLUMNS = new Set([
  "active_boroughs",
  "active_days",
  "active_pickup_zones",
  "avg_fare_amount",
  "avg_passenger_count",
  "avg_revenue_per_day",
  "avg_revenue_per_trip",
  "avg_speed_mph",
  "avg_tip_amount",
  "avg_total_amount",
  "avg_trip_distance",
  "avg_trip_duration_min",
  "avg_trips_per_active_day",
  "avg_trips_per_day",
  "cash_share",
  "cash_trip_count",
  "credit_card_share",
  "credit_card_trip_count",
  "day",
  "day_of_week",
  "DOLocationID",
  "hour",
  "is_weekend",
  "month",
  "PULocationID",
  "predicted",
  "prediction",
  "predicted_trip_count",
  "actual",
  "actual_trip_count",
  "route_rank",
  "time_period_order",
  "total_amount",
  "total_airport_fee",
  "total_fare_amount",
  "total_passenger_count",
  "total_revenue",
  "total_tip_amount",
  "total_tolls_amount",
  "total_trip_distance",
  "total_trip_duration_min",
  "total_trips",
  "trip_count",
  "year",
  "zone_rank_in_hour",
]);

const METRIC_LABELS = {
  total_trips: "Trips",
  total_revenue: "Revenue",
  avg_revenue_per_trip: "Revenue per trip",
  avg_trip_distance: "Distance per trip",
};

const COLORS = {
  teal: "#138a8a",
  blue: "#2f6fbb",
  gold: "#b7791f",
  rose: "#c2415d",
  green: "#2f855a",
  ink: "#17202a",
  muted: "#647184",
  line: "#dce2ea",
};

const state = {
  dateScope: "primary",
  dailyMetric: "total_trips",
};

let dashboardData = null;
let primaryYears = [];

function toNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseRow(row) {
  for (const key of Object.keys(row)) {
    if (NUMERIC_COLUMNS.has(key)) {
      row[key] = toNumber(row[key]);
    }
  }
  return row;
}

function compact(value) {
  return d3.format(".3s")(value || 0).replace("G", "B");
}

function integer(value) {
  return d3.format(",.0f")(value || 0);
}

function money(value) {
  if (!Number.isFinite(value)) {
    return "$0";
  }
  if (Math.abs(value) >= 1000000) {
    return `$${d3.format(".2s")(value).replace("G", "B")}`;
  }
  return d3.format("$,.0f")(value);
}

function decimal(value, digits = 1) {
  return d3.format(`,.${digits}f`)(value || 0);
}

function parseDate(value) {
  return value ? new Date(`${value}T00:00:00`) : null;
}

function parseDateTime(row) {
  const date = row.pickup_date || row.date || row.ds;
  const hour = row.hour ?? 0;
  if (!date) {
    return null;
  }
  return new Date(`${date}T${String(hour).padStart(2, "0")}:00:00`);
}

function clear(selector) {
  const element = document.querySelector(selector);
  if (element) {
    element.innerHTML = "";
  }
  return element;
}

function showEmpty(selector, title, message) {
  const element = clear(selector);
  if (!element) {
    return;
  }

  const wrapper = document.createElement("div");
  wrapper.className = "empty-state";
  wrapper.innerHTML = `<div><strong>${title}</strong><span>${message}</span></div>`;
  element.appendChild(wrapper);
}

function createSvg(selector, height = 320, margin = { top: 18, right: 22, bottom: 38, left: 58 }) {
  const element = clear(selector);
  const width = Math.max(320, element.clientWidth || 760);
  const svg = d3
    .select(element)
    .append("svg")
    .attr("viewBox", `0 0 ${width} ${height}`)
    .attr("role", "img");

  return {
    svg,
    width,
    height,
    margin,
    innerWidth: width - margin.left - margin.right,
    innerHeight: height - margin.top - margin.bottom,
    plot: svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`),
  };
}

function addYAxisGrid(plot, scale, width, ticks = 5) {
  plot
    .append("g")
    .attr("class", "grid")
    .call(d3.axisLeft(scale).ticks(ticks).tickSize(-width).tickFormat(""))
    .call((group) => group.selectAll("line").attr("stroke-dasharray", "2 4"));
}

function addXAxisGrid(plot, scale, height, ticks = 5) {
  plot
    .append("g")
    .attr("class", "grid")
    .attr("transform", `translate(0,${height})`)
    .call(d3.axisBottom(scale).ticks(ticks).tickSize(-height).tickFormat(""))
    .call((group) => group.selectAll("line").attr("stroke-dasharray", "2 4"));
}

function inferPrimaryYears(dailyRows) {
  const counts = d3.rollups(
    dailyRows,
    (rows) => rows.length,
    (row) => row.year
  );

  const years = counts
    .filter(([year, count]) => Number.isFinite(year) && count >= 30)
    .map(([year]) => year)
    .sort(d3.ascending);

  return years.length ? years : counts.map(([year]) => year).sort(d3.ascending);
}

function setDateScopeOptions() {
  const select = document.querySelector("#dateScope");
  const label =
    primaryYears.length > 1
      ? `${primaryYears[0]}-${primaryYears[primaryYears.length - 1]}`
      : `${primaryYears[0] || "Primary years"}`;

  select.innerHTML = "";
  select.add(new Option(label, "primary", true, true));
  select.add(new Option("All records", "all"));
}

function scopedDailyRows() {
  if (!dashboardData) {
    return [];
  }
  if (state.dateScope === "all") {
    return dashboardData.daily;
  }
  const yearSet = new Set(primaryYears);
  return dashboardData.daily.filter((row) => yearSet.has(row.year));
}

function scopedMonthlyRows() {
  const dailyYearSet = new Set(scopedDailyRows().map((row) => `${row.year}-${row.month}`));
  return dashboardData.monthly.filter((row) => dailyYearSet.has(`${row.year}-${row.month}`));
}

function renderKpis() {
  const grid = clear("#kpiGrid");
  const daily = scopedDailyRows();
  const kpi = dashboardData.kpi[0] || {};
  const totalTrips = d3.sum(daily, (row) => row.total_trips || 0);
  const totalRevenue = d3.sum(daily, (row) => row.total_revenue || 0);
  const dates = daily.map((row) => parseDate(row.pickup_date)).filter(Boolean).sort(d3.ascending);
  const startDate = dates[0] ? d3.timeFormat("%Y-%m-%d")(dates[0]) : kpi.start_date || "n/a";
  const endDate = dates.at(-1) ? d3.timeFormat("%Y-%m-%d")(dates.at(-1)) : kpi.end_date || "n/a";

  const cards = [
    ["Total trips", compact(totalTrips), `${integer(daily.length)} active days`],
    ["Total revenue", money(totalRevenue), `${money(totalRevenue / Math.max(daily.length, 1))} per day`],
    ["Average trips", integer(totalTrips / Math.max(daily.length, 1)), "per active day"],
    ["Pickup zones", integer(kpi.active_pickup_zones), `${integer(kpi.active_boroughs)} borough groups`],
    ["Date range", `${startDate}`, `to ${endDate}`],
  ];

  for (const [label, value, note] of cards) {
    const card = document.createElement("article");
    card.className = "kpi-card";
    card.innerHTML = `
      <div class="kpi-label">${label}</div>
      <div class="kpi-value">${value}</div>
      <div class="kpi-note">${note}</div>
    `;
    grid.appendChild(card);
  }
}

function renderDailyTrend() {
  const metric = state.dailyMetric;
  const data = scopedDailyRows()
    .map((row) => ({ ...row, date: parseDate(row.pickup_date) }))
    .filter((row) => row.date && Number.isFinite(row[metric]))
    .sort((a, b) => d3.ascending(a.date, b.date));

  if (!data.length) {
    showEmpty("#dailyTrend", "No daily records", "Run Stage 4 with CSV output first.");
    return;
  }

  const chart = createSvg("#dailyTrend", 370, { top: 18, right: 26, bottom: 42, left: 68 });
  const x = d3.scaleTime().domain(d3.extent(data, (row) => row.date)).range([0, chart.innerWidth]);
  const y = d3
    .scaleLinear()
    .domain([0, d3.max(data, (row) => row[metric]) || 1])
    .nice()
    .range([chart.innerHeight, 0]);

  addYAxisGrid(chart.plot, y, chart.innerWidth);

  const line = d3
    .line()
    .defined((row) => Number.isFinite(row[metric]))
    .x((row) => x(row.date))
    .y((row) => y(row[metric]))
    .curve(d3.curveMonotoneX);

  chart.plot
    .append("path")
    .datum(data)
    .attr("fill", "none")
    .attr("stroke", COLORS.teal)
    .attr("stroke-width", 2.4)
    .attr("d", line);

  chart.plot
    .append("g")
    .attr("class", "axis")
    .attr("transform", `translate(0,${chart.innerHeight})`)
    .call(d3.axisBottom(x).ticks(Math.min(8, data.length)).tickSizeOuter(0));

  chart.plot.append("g").attr("class", "axis").call(d3.axisLeft(y).ticks(5).tickFormat(compact));

  chart.svg
    .append("text")
    .attr("class", "chart-title")
    .attr("x", chart.margin.left)
    .attr("y", chart.height - 6)
    .text(METRIC_LABELS[metric]);
}

function renderHourlyDemand() {
  const data = dashboardData.hourly
    .filter((row) => Number.isFinite(row.hour) && Number.isFinite(row.total_trips))
    .sort((a, b) => d3.ascending(a.hour, b.hour));

  if (!data.length) {
    showEmpty("#hourlyDemand", "No hourly records", "Hourly demand CSV is missing.");
    return;
  }

  const chart = createSvg("#hourlyDemand", 320, { top: 18, right: 18, bottom: 42, left: 64 });
  const x = d3.scaleBand().domain(data.map((row) => row.hour)).range([0, chart.innerWidth]).padding(0.16);
  const y = d3
    .scaleLinear()
    .domain([0, d3.max(data, (row) => row.total_trips) || 1])
    .nice()
    .range([chart.innerHeight, 0]);

  addYAxisGrid(chart.plot, y, chart.innerWidth);

  chart.plot
    .selectAll("rect")
    .data(data)
    .join("rect")
    .attr("x", (row) => x(row.hour))
    .attr("y", (row) => y(row.total_trips))
    .attr("width", x.bandwidth())
    .attr("height", (row) => chart.innerHeight - y(row.total_trips))
    .attr("rx", 3)
    .attr("fill", COLORS.blue);

  chart.plot
    .append("g")
    .attr("class", "axis")
    .attr("transform", `translate(0,${chart.innerHeight})`)
    .call(d3.axisBottom(x).tickValues(data.map((row) => row.hour).filter((hour) => hour % 3 === 0)));

  chart.plot.append("g").attr("class", "axis").call(d3.axisLeft(y).ticks(5).tickFormat(compact));
}

function renderTopZones() {
  const data = dashboardData.zones
    .filter((row) => row.pickup_zone && row.pickup_zone !== "N/A")
    .sort((a, b) => d3.descending(a.total_trips, b.total_trips))
    .slice(0, 12)
    .reverse();

  if (!data.length) {
    showEmpty("#topZones", "No zone records", "Top zone CSV is missing.");
    return;
  }

  const chart = createSvg("#topZones", 360, { top: 14, right: 28, bottom: 34, left: 150 });
  const x = d3
    .scaleLinear()
    .domain([0, d3.max(data, (row) => row.total_trips) || 1])
    .nice()
    .range([0, chart.innerWidth]);
  const y = d3
    .scaleBand()
    .domain(data.map((row) => row.pickup_zone))
    .range([chart.innerHeight, 0])
    .padding(0.18);

  addXAxisGrid(chart.plot, x, chart.innerHeight, 4);

  chart.plot
    .selectAll("rect")
    .data(data)
    .join("rect")
    .attr("x", 0)
    .attr("y", (row) => y(row.pickup_zone))
    .attr("width", (row) => x(row.total_trips))
    .attr("height", y.bandwidth())
    .attr("rx", 3)
    .attr("fill", COLORS.green);

  chart.plot
    .append("g")
    .attr("class", "axis")
    .attr("transform", `translate(0,${chart.innerHeight})`)
    .call(d3.axisBottom(x).ticks(4).tickFormat(compact));

  chart.plot.append("g").attr("class", "axis").call(d3.axisLeft(y).tickSizeOuter(0));
}

function renderWeekdayHeatmap() {
  const data = dashboardData.heatmap.filter(
    (row) => Number.isFinite(row.day_of_week) && Number.isFinite(row.hour)
  );

  if (!data.length) {
    showEmpty("#weekdayHeatmap", "No heatmap records", "Weekday-hour heatmap CSV is missing.");
    return;
  }

  const dayOrder = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
  const hours = d3.range(24);
  const chart = createSvg("#weekdayHeatmap", 370, { top: 22, right: 22, bottom: 42, left: 58 });
  const x = d3.scaleBand().domain(hours).range([0, chart.innerWidth]).padding(0.04);
  const y = d3.scaleBand().domain(dayOrder).range([0, chart.innerHeight]).padding(0.06);
  const color = d3
    .scaleSequential()
    .interpolator(d3.interpolateYlOrRd)
    .domain([0, d3.max(data, (row) => row.total_trips) || 1]);

  chart.plot
    .selectAll("rect")
    .data(data)
    .join("rect")
    .attr("x", (row) => x(row.hour))
    .attr("y", (row) => y(row.weekday_name))
    .attr("width", x.bandwidth())
    .attr("height", y.bandwidth())
    .attr("rx", 2)
    .attr("fill", (row) => color(row.total_trips || 0));

  chart.plot
    .append("g")
    .attr("class", "axis")
    .attr("transform", `translate(0,${chart.innerHeight})`)
    .call(d3.axisBottom(x).tickValues(hours.filter((hour) => hour % 2 === 0)));

  chart.plot.append("g").attr("class", "axis").call(d3.axisLeft(y).tickSizeOuter(0));
}

function renderBoroughPattern() {
  const rows = dashboardData.borough.filter(
    (row) =>
      row.pickup_borough &&
      row.pickup_borough !== "Unknown" &&
      Number.isFinite(row.hour) &&
      Number.isFinite(row.total_trips)
  );

  if (!rows.length) {
    showEmpty("#boroughPattern", "No borough records", "Borough pattern CSV is missing.");
    return;
  }

  const totals = d3.rollups(
    rows,
    (values) => d3.sum(values, (row) => row.total_trips || 0),
    (row) => row.pickup_borough
  );
  const boroughs = totals
    .sort((a, b) => d3.descending(a[1], b[1]))
    .slice(0, 5)
    .map(([borough]) => borough);
  const boroughSet = new Set(boroughs);
  const grouped = d3.group(
    rows.filter((row) => boroughSet.has(row.pickup_borough)),
    (row) => row.pickup_borough
  );

  const chart = createSvg("#boroughPattern", 330, { top: 18, right: 88, bottom: 40, left: 62 });
  const x = d3.scaleLinear().domain([0, 23]).range([0, chart.innerWidth]);
  const y = d3
    .scaleLinear()
    .domain([0, d3.max(rows, (row) => (boroughSet.has(row.pickup_borough) ? row.total_trips : 0)) || 1])
    .nice()
    .range([chart.innerHeight, 0]);
  const color = d3
    .scaleOrdinal()
    .domain(boroughs)
    .range([COLORS.teal, COLORS.blue, COLORS.gold, COLORS.rose, COLORS.green]);

  addYAxisGrid(chart.plot, y, chart.innerWidth);

  const line = d3
    .line()
    .x((row) => x(row.hour))
    .y((row) => y(row.total_trips))
    .curve(d3.curveMonotoneX);

  for (const [borough, values] of grouped) {
    chart.plot
      .append("path")
      .datum(values.sort((a, b) => d3.ascending(a.hour, b.hour)))
      .attr("fill", "none")
      .attr("stroke", color(borough))
      .attr("stroke-width", 2.2)
      .attr("d", line);
  }

  chart.plot
    .append("g")
    .attr("class", "axis")
    .attr("transform", `translate(0,${chart.innerHeight})`)
    .call(d3.axisBottom(x).ticks(8).tickFormat(d3.format("02d")));

  chart.plot.append("g").attr("class", "axis").call(d3.axisLeft(y).ticks(5).tickFormat(compact));

  const legend = chart.svg.append("g").attr("class", "legend").attr("transform", `translate(${chart.width - 78},24)`);
  boroughs.forEach((borough, index) => {
    const item = legend.append("g").attr("transform", `translate(0,${index * 20})`);
    item.append("rect").attr("width", 10).attr("height", 10).attr("rx", 2).attr("fill", color(borough));
    item.append("text").attr("x", 16).attr("y", 9).text(borough);
  });
}

function renderRushHour() {
  const data = dashboardData.rush
    .filter((row) => Number.isFinite(row.total_trips))
    .sort((a, b) => d3.ascending(a.time_period_order, b.time_period_order));

  if (!data.length) {
    showEmpty("#rushHour", "No rush-hour records", "Rush-hour summary CSV is missing.");
    return;
  }

  const labels = data.map((row) => row.time_period.replaceAll("_", " "));
  const chart = createSvg("#rushHour", 330, { top: 16, right: 18, bottom: 86, left: 64 });
  const x = d3.scaleBand().domain(labels).range([0, chart.innerWidth]).padding(0.18);
  const y = d3
    .scaleLinear()
    .domain([0, d3.max(data, (row) => row.total_trips) || 1])
    .nice()
    .range([chart.innerHeight, 0]);

  addYAxisGrid(chart.plot, y, chart.innerWidth);

  chart.plot
    .selectAll("rect")
    .data(data)
    .join("rect")
    .attr("x", (row) => x(row.time_period.replaceAll("_", " ")))
    .attr("y", (row) => y(row.total_trips))
    .attr("width", x.bandwidth())
    .attr("height", (row) => chart.innerHeight - y(row.total_trips))
    .attr("rx", 3)
    .attr("fill", COLORS.gold);

  chart.plot
    .append("g")
    .attr("class", "axis")
    .attr("transform", `translate(0,${chart.innerHeight})`)
    .call(d3.axisBottom(x).tickSizeOuter(0))
    .selectAll("text")
    .attr("text-anchor", "end")
    .attr("transform", "rotate(-32)")
    .attr("dx", "-0.6em")
    .attr("dy", "0.2em");

  chart.plot.append("g").attr("class", "axis").call(d3.axisLeft(y).ticks(5).tickFormat(compact));
}

function normalizeForecastRows(rows) {
  return rows
    .map((row) => ({
      ...row,
      dateTime: parseDateTime(row),
      actualValue: row.actual_trip_count ?? row.actual ?? row.trip_count,
      predictedValue: row.predicted_trip_count ?? row.predicted ?? row.prediction,
      zone: row.pickup_zone || row.zone || "All zones",
    }))
    .filter((row) => row.dateTime && Number.isFinite(row.predictedValue));
}

function renderForecast() {
  const status = document.querySelector("#forecastStatus");
  const data = normalizeForecastRows(dashboardData.forecast).sort((a, b) =>
    d3.ascending(a.dateTime, b.dateTime)
  );

  if (!data.length) {
    status.textContent = "Stage5 pending";
    status.classList.add("pending");
    showEmpty(
      "#forecastChart",
      "Forecast output not found",
      "Add outputs/predictions/forecast_demand.csv after Stage5 is ready."
    );
    return;
  }

  status.textContent = "Loaded";
  status.classList.remove("pending");

  const zoneTotals = d3.rollups(
    data,
    (rows) => d3.sum(rows, (row) => row.actualValue || row.predictedValue || 0),
    (row) => row.zone
  );
  const selectedZone = zoneTotals.sort((a, b) => d3.descending(a[1], b[1]))[0][0];
  const rows = data.filter((row) => row.zone === selectedZone).slice(0, 160);

  const chart = createSvg("#forecastChart", 330, { top: 18, right: 96, bottom: 42, left: 62 });
  const x = d3.scaleTime().domain(d3.extent(rows, (row) => row.dateTime)).range([0, chart.innerWidth]);
  const y = d3
    .scaleLinear()
    .domain([
      0,
      d3.max(rows, (row) => Math.max(row.actualValue || 0, row.predictedValue || 0)) || 1,
    ])
    .nice()
    .range([chart.innerHeight, 0]);

  addYAxisGrid(chart.plot, y, chart.innerWidth);

  const lineFor = (key) =>
    d3
      .line()
      .defined((row) => Number.isFinite(row[key]))
      .x((row) => x(row.dateTime))
      .y((row) => y(row[key]))
      .curve(d3.curveMonotoneX);

  chart.plot
    .append("path")
    .datum(rows)
    .attr("fill", "none")
    .attr("stroke", COLORS.ink)
    .attr("stroke-width", 2.2)
    .attr("d", lineFor("actualValue"));

  chart.plot
    .append("path")
    .datum(rows)
    .attr("fill", "none")
    .attr("stroke", COLORS.rose)
    .attr("stroke-width", 2.2)
    .attr("stroke-dasharray", "6 4")
    .attr("d", lineFor("predictedValue"));

  chart.plot
    .append("g")
    .attr("class", "axis")
    .attr("transform", `translate(0,${chart.innerHeight})`)
    .call(d3.axisBottom(x).ticks(6));

  chart.plot.append("g").attr("class", "axis").call(d3.axisLeft(y).ticks(5).tickFormat(compact));

  const legend = chart.svg.append("g").attr("class", "legend").attr("transform", `translate(${chart.width - 84},24)`);
  [
    ["Actual", COLORS.ink, "0"],
    ["Forecast", COLORS.rose, "6 4"],
  ].forEach(([label, color, dash], index) => {
    const item = legend.append("g").attr("transform", `translate(0,${index * 22})`);
    item
      .append("line")
      .attr("x1", 0)
      .attr("x2", 24)
      .attr("y1", 6)
      .attr("y2", 6)
      .attr("stroke", color)
      .attr("stroke-width", 2.2)
      .attr("stroke-dasharray", dash);
    item.append("text").attr("x", 30).attr("y", 10).text(label);
  });

  chart.svg
    .append("text")
    .attr("class", "chart-title")
    .attr("x", chart.margin.left)
    .attr("y", chart.height - 6)
    .text(selectedZone);
}

function renderRoutesTable() {
  const tbody = clear("#routesTable");
  const rows = dashboardData.routes
    .filter((row) => row.route_name && row.route_name !== "N/A -> N/A")
    .sort((a, b) => d3.ascending(a.route_rank, b.route_rank))
    .slice(0, 10);

  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="6">No top route records found.</td>`;
    tbody.appendChild(tr);
    return;
  }

  for (const row of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${integer(row.route_rank)}</td>
      <td>${row.route_name}</td>
      <td class="numeric">${integer(row.trip_count)}</td>
      <td class="numeric">${decimal(row.avg_trip_distance, 2)}</td>
      <td class="numeric">${decimal(row.avg_trip_duration_min, 1)}</td>
      <td class="numeric">${money(row.total_revenue)}</td>
    `;
    tbody.appendChild(tr);
  }
}

function renderDashboard() {
  renderKpis();
  renderDailyTrend();
  renderHourlyDemand();
  renderTopZones();
  renderWeekdayHeatmap();
  renderBoroughPattern();
  renderRushHour();
  renderForecast();
  renderRoutesTable();
}

async function loadDashboardData() {
  if (!window.d3) {
    showEmpty("#dailyTrend", "D3 failed to load", "Check the CDN connection or use a local D3 file.");
    return;
  }

  const entries = await Promise.all(
    Object.entries(FILES).map(async ([key, file]) => {
      const rows = await d3.csv(`data/${file}`, parseRow);
      return [key, rows];
    })
  );

  dashboardData = Object.fromEntries(entries);
  primaryYears = inferPrimaryYears(dashboardData.daily);
  setDateScopeOptions();
  renderDashboard();
}

function debounce(callback, delay = 180) {
  let handle = null;
  return (...args) => {
    window.clearTimeout(handle);
    handle = window.setTimeout(() => callback(...args), delay);
  };
}

document.addEventListener("change", (event) => {
  if (event.target.matches("#dateScope")) {
    state.dateScope = event.target.value;
    renderDashboard();
  }

  if (event.target.matches("#dailyMetric")) {
    state.dailyMetric = event.target.value;
    renderDashboard();
  }
});

window.addEventListener(
  "resize",
  debounce(() => {
    if (dashboardData) {
      renderDashboard();
    }
  })
);

loadDashboardData().catch((error) => {
  console.error(error);
  showEmpty("#dailyTrend", "Dashboard data not ready", "Run python src/visualization/prepare_dashboard_data.py.");
});
