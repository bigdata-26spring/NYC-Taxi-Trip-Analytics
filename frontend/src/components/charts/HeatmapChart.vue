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
  valueKey: {
    type: String,
    required: true,
  },
  xDomain: {
    type: Array,
    default: () => [],
  },
  yDomain: {
    type: Array,
    default: () => [],
  },
  height: {
    type: Number,
    default: 360,
  },
  emptyTitle: {
    type: String,
    default: "No heatmap data",
  },
});

const root = ref(null);
let resizeObserver;

function uniqueDomain(rows, key) {
  return [...new Set(rows.map((row) => row[key]))].sort((a, b) => {
    const na = Number(a);
    const nb = Number(b);
    return Number.isFinite(na) && Number.isFinite(nb) ? d3.ascending(na, nb) : d3.ascending(a, b);
  });
}

function render() {
  if (!root.value || !props.data.length) {
    return;
  }

  const width = Math.max(340, root.value.clientWidth || 760);
  const height = props.height;
  const margin = { top: 18, right: 20, bottom: 44, left: 86 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const rows = props.data
    .map((row) => ({ ...row, __value: Number(row[props.valueKey]) }))
    .filter((row) => row[props.xKey] !== undefined && row[props.yKey] !== undefined && Number.isFinite(row.__value));
  const xDomain = props.xDomain.length ? props.xDomain : uniqueDomain(rows, props.xKey);
  const yDomain = props.yDomain.length ? props.yDomain : uniqueDomain(rows, props.yKey);

  d3.select(root.value).selectAll("svg").remove();
  d3.select(root.value).selectAll(".chart-tooltip").remove();

  if (!rows.length) {
    return;
  }

  const x = d3.scaleBand().domain(xDomain).range([0, innerWidth]).padding(0.04);
  const y = d3.scaleBand().domain(yDomain).range([0, innerHeight]).padding(0.06);
  const color = d3
    .scaleSequential(d3.interpolateYlGnBu)
    .domain([0, d3.max(rows, (row) => row.__value) || 1]);
  const tooltip = d3.select(root.value).append("div").attr("class", "chart-tooltip");
  const svg = d3.select(root.value).append("svg").attr("viewBox", `0 0 ${width} ${height}`);
  const plot = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  plot
    .selectAll("rect")
    .data(rows)
    .join("rect")
    .attr("x", (row) => x(row[props.xKey]))
    .attr("y", (row) => y(row[props.yKey]))
    .attr("width", x.bandwidth())
    .attr("height", y.bandwidth())
    .attr("rx", 2)
    .attr("fill", (row) => color(row.__value))
    .on("mouseenter", function (event, row) {
      d3.select(this).attr("stroke", "#17202a").attr("stroke-width", 1.5);
      tooltip
        .style("opacity", 1)
        .html(
          `<strong>${row[props.yKey]} / ${row[props.xKey]}</strong>
          <b>${d3.format(",")(row.__value)}</b>`
        );
    })
    .on("mousemove", (event) => {
      tooltip.style("left", `${event.offsetX + 14}px`).style("top", `${event.offsetY + 14}px`);
    })
    .on("mouseleave", function () {
      d3.select(this).attr("stroke", null);
      tooltip.style("opacity", 0);
    })
    .append("title")
    .text((row) => `${row[props.yKey]} / ${row[props.xKey]}: ${d3.format(",")(row.__value)}`);

  plot
    .append("g")
    .attr("class", "axis")
    .attr("transform", `translate(0,${innerHeight})`)
    .call(d3.axisBottom(x).tickSizeOuter(0));

  plot.append("g").attr("class", "axis").call(d3.axisLeft(y).tickSizeOuter(0));
}

watch(() => [props.data, props.xKey, props.yKey, props.valueKey], render, { deep: true });

onMounted(() => {
  render();
  resizeObserver = new ResizeObserver(render);
  resizeObserver.observe(root.value);
});

onBeforeUnmount(() => resizeObserver?.disconnect());
</script>
