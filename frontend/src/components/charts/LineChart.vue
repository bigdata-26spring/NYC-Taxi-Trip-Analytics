<template>
  <div ref="root" class="chart-shell">
    <EmptyState v-if="!data.length" :title="emptyTitle" />
  </div>
</template>

<script setup>
import * as d3 from "d3";
import { onBeforeUnmount, onMounted, ref, watch } from "vue";

import EmptyState from "./EmptyState.vue";

const props = defineProps({
  data: {
    type: Array,
    default: () => [],
  },
  xKey: {
    type: String,
    required: true,
  },
  yKey: {
    type: String,
    required: true,
  },
  seriesKey: {
    type: String,
    default: "",
  },
  height: {
    type: Number,
    default: 320,
  },
  emptyTitle: {
    type: String,
    default: "No trend data",
  },
});

const root = ref(null);
let resizeObserver;

function parseX(value) {
  if (value instanceof Date) {
    return value;
  }
  if (typeof value === "number") {
    return value;
  }
  if (/^\d{4}-\d{2}$/.test(String(value))) {
    return new Date(`${value}-01T00:00:00`);
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(String(value))) {
    return new Date(`${value}T00:00:00`);
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : value;
}

function render() {
  if (!root.value || !props.data.length) {
    return;
  }

  const width = Math.max(360, root.value.clientWidth || 720);
  const height = props.height;
  const margin = { top: 18, right: props.seriesKey ? 110 : 24, bottom: 38, left: 64 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const rows = props.data
    .map((row) => ({
      ...row,
      __x: parseX(row[props.xKey]),
      __y: Number(row[props.yKey]),
    }))
    .filter((row) => Number.isFinite(row.__y) && row.__x !== null && row.__x !== undefined);

  d3.select(root.value).selectAll("svg").remove();
  d3.select(root.value).selectAll(".chart-tooltip").remove();

  if (!rows.length) {
    return;
  }

  const isTime = rows[0].__x instanceof Date;
  const isNumber = typeof rows[0].__x === "number";
  const x = isTime
    ? d3.scaleTime().domain(d3.extent(rows, (row) => row.__x)).range([0, innerWidth])
    : isNumber
      ? d3.scaleLinear().domain(d3.extent(rows, (row) => row.__x)).nice().range([0, innerWidth])
      : d3.scalePoint().domain([...new Set(rows.map((row) => row.__x))]).range([0, innerWidth]);
  const y = d3
    .scaleLinear()
    .domain([0, d3.max(rows, (row) => row.__y) || 1])
    .nice()
    .range([innerHeight, 0]);
  const series = props.seriesKey
    ? Array.from(d3.group(rows, (row) => row[props.seriesKey]), ([key, values]) => ({
        key,
        values: values.sort((a, b) => d3.ascending(a.__x, b.__x)),
      }))
    : [{ key: props.yKey, values: rows.sort((a, b) => d3.ascending(a.__x, b.__x)) }];
  const color = d3
    .scaleOrdinal()
    .domain(series.map((item) => item.key))
    .range(["#0f1720", "#21bfd0", "#f7c948", "#d44a5f", "#23845a", "#7c6f5f", "#6b7280"]);

  const tooltip = d3.select(root.value).append("div").attr("class", "chart-tooltip");
  const svg = d3
    .select(root.value)
    .append("svg")
    .attr("viewBox", `0 0 ${width} ${height}`)
    .attr("role", "img");
  const plot = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  plot
    .append("g")
    .attr("class", "grid")
    .call(d3.axisLeft(y).ticks(5).tickSize(-innerWidth).tickFormat(""))
    .call((group) => group.selectAll("line").attr("stroke-dasharray", "2 4"));

  const line = d3
    .line()
    .x((row) => x(row.__x))
    .y((row) => y(row.__y))
    .curve(d3.curveMonotoneX);

  plot
    .selectAll(".line-series")
    .data(series)
    .join("path")
    .attr("class", "line-series")
    .attr("fill", "none")
    .attr("stroke", (item) => color(item.key))
    .attr("stroke-width", 2.4)
    .attr("d", (item) => line(item.values));

  plot
    .append("g")
    .attr("class", "axis")
    .attr("transform", `translate(0,${innerHeight})`)
    .call(d3.axisBottom(x).ticks(6).tickSizeOuter(0));

  plot.append("g").attr("class", "axis").call(d3.axisLeft(y).ticks(5).tickFormat(d3.format(".2s")));

  if (props.seriesKey) {
    const legend = svg.append("g").attr("class", "legend").attr("transform", `translate(${width - 96},24)`);
    series.slice(0, 7).forEach((item, index) => {
      const row = legend.append("g").attr("transform", `translate(0,${index * 20})`);
      row.append("rect").attr("width", 10).attr("height", 10).attr("rx", 2).attr("fill", color(item.key));
      row.append("text").attr("x", 16).attr("y", 9).text(item.key);
    });
  }

  const focus = plot.append("g").style("display", "none");
  focus.append("line").attr("y1", 0).attr("y2", innerHeight).attr("stroke", "#17202a").attr("stroke-dasharray", "3 4");
  focus.append("circle").attr("r", 4).attr("fill", "#17202a").attr("stroke", "#ffffff").attr("stroke-width", 2);

  plot
    .append("rect")
    .attr("width", innerWidth)
    .attr("height", innerHeight)
    .attr("fill", "transparent")
    .on("mouseenter", () => focus.style("display", null))
    .on("mousemove", (event) => {
      const [mx] = d3.pointer(event);
      const nearest = rows.reduce((best, row) => {
        const distance = Math.abs(x(row.__x) - mx);
        return distance < best.distance ? { row, distance } : best;
      }, { row: rows[0], distance: Infinity }).row;
      const px = x(nearest.__x);
      const py = y(nearest.__y);
      focus.attr("transform", `translate(${px},0)`);
      focus.select("circle").attr("cy", py);
      tooltip
        .style("opacity", 1)
        .style("left", `${event.offsetX + 14}px`)
        .style("top", `${event.offsetY + 14}px`)
        .html(
          `<strong>${props.seriesKey ? nearest[props.seriesKey] : props.yKey}</strong>
          <span>${nearest[props.xKey]}</span>
          <b>${d3.format(",.2f")(nearest.__y)}</b>`
        );
    })
    .on("mouseleave", () => {
      focus.style("display", "none");
      tooltip.style("opacity", 0);
    });
}

watch(() => [props.data, props.xKey, props.yKey, props.seriesKey], render, { deep: true });

onMounted(() => {
  render();
  resizeObserver = new ResizeObserver(render);
  resizeObserver.observe(root.value);
});

onBeforeUnmount(() => {
  resizeObserver?.disconnect();
});
</script>
