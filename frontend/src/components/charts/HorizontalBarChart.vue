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
  labelKey: {
    type: String,
    required: true,
  },
  valueKey: {
    type: String,
    required: true,
  },
  color: {
    type: String,
    default: "#2f855a",
  },
  limit: {
    type: Number,
    default: 12,
  },
  height: {
    type: Number,
    default: 360,
  },
  emptyTitle: {
    type: String,
    default: "No ranking data",
  },
});

const root = ref(null);
let resizeObserver;

function render() {
  if (!root.value || !props.data.length) {
    return;
  }

  const width = Math.max(340, root.value.clientWidth || 680);
  const height = props.height;
  const margin = { top: 14, right: 32, bottom: 34, left: 164 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const rows = props.data
    .map((row) => ({
      label: row[props.labelKey],
      value: Number(row[props.valueKey]),
    }))
    .filter((row) => row.label && Number.isFinite(row.value))
    .sort((a, b) => d3.descending(a.value, b.value))
    .slice(0, props.limit)
    .reverse();

  d3.select(root.value).selectAll("svg").remove();
  d3.select(root.value).selectAll(".chart-tooltip").remove();

  if (!rows.length) {
    return;
  }

  const x = d3
    .scaleLinear()
    .domain([0, d3.max(rows, (row) => row.value) || 1])
    .nice()
    .range([0, innerWidth]);
  const y = d3.scaleBand().domain(rows.map((row) => row.label)).range([innerHeight, 0]).padding(0.18);
  const tooltip = d3.select(root.value).append("div").attr("class", "chart-tooltip");
  const svg = d3.select(root.value).append("svg").attr("viewBox", `0 0 ${width} ${height}`);
  const plot = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  plot
    .append("g")
    .attr("class", "grid")
    .attr("transform", `translate(0,${innerHeight})`)
    .call(d3.axisBottom(x).ticks(4).tickSize(-innerHeight).tickFormat(""))
    .call((group) => group.selectAll("line").attr("stroke-dasharray", "2 4"));

  plot
    .selectAll("rect")
    .data(rows)
    .join("rect")
    .attr("x", 0)
    .attr("y", (row) => y(row.label))
    .attr("width", (row) => x(row.value))
    .attr("height", y.bandwidth())
    .attr("rx", 3)
    .attr("fill", props.color)
    .on("mouseenter", function (event, row) {
      d3.select(this).attr("opacity", 0.78);
      tooltip
        .style("opacity", 1)
        .html(`<strong>${row.label}</strong><b>${d3.format(",.2f")(row.value)}</b>`);
    })
    .on("mousemove", (event) => {
      tooltip.style("left", `${event.offsetX + 14}px`).style("top", `${event.offsetY + 14}px`);
    })
    .on("mouseleave", function () {
      d3.select(this).attr("opacity", 1);
      tooltip.style("opacity", 0);
    });

  plot
    .append("g")
    .attr("class", "axis")
    .attr("transform", `translate(0,${innerHeight})`)
    .call(d3.axisBottom(x).ticks(4).tickFormat(d3.format(".2s")));

  plot.append("g").attr("class", "axis").call(d3.axisLeft(y).tickSizeOuter(0));
}

watch(() => [props.data, props.labelKey, props.valueKey, props.limit], render, { deep: true });

onMounted(() => {
  render();
  resizeObserver = new ResizeObserver(render);
  resizeObserver.observe(root.value);
});

onBeforeUnmount(() => resizeObserver?.disconnect());
</script>
