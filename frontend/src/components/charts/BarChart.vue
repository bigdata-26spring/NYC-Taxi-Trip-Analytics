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
  color: {
    type: String,
    default: "#2f6fbb",
  },
  height: {
    type: Number,
    default: 310,
  },
  emptyTitle: {
    type: String,
    default: "No bar data",
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
  const margin = { top: 18, right: 18, bottom: 48, left: 64 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const rows = props.data
    .map((row) => ({
      ...row,
      __x: row[props.xKey],
      __y: Number(row[props.yKey]),
    }))
    .filter((row) => row.__x !== null && row.__x !== undefined && Number.isFinite(row.__y));

  d3.select(root.value).selectAll("svg").remove();
  d3.select(root.value).selectAll(".chart-tooltip").remove();

  if (!rows.length) {
    return;
  }

  const x = d3.scaleBand().domain(rows.map((row) => row.__x)).range([0, innerWidth]).padding(0.16);
  const y = d3
    .scaleLinear()
    .domain([0, d3.max(rows, (row) => row.__y) || 1])
    .nice()
    .range([innerHeight, 0]);
  const tooltip = d3.select(root.value).append("div").attr("class", "chart-tooltip");
  const svg = d3.select(root.value).append("svg").attr("viewBox", `0 0 ${width} ${height}`);
  const plot = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  plot
    .append("g")
    .attr("class", "grid")
    .call(d3.axisLeft(y).ticks(5).tickSize(-innerWidth).tickFormat(""))
    .call((group) => group.selectAll("line").attr("stroke-dasharray", "2 4"));

  plot
    .selectAll("rect")
    .data(rows)
    .join("rect")
    .attr("x", (row) => x(row.__x))
    .attr("y", (row) => y(row.__y))
    .attr("width", x.bandwidth())
    .attr("height", (row) => innerHeight - y(row.__y))
    .attr("rx", 3)
    .attr("fill", props.color)
    .on("mouseenter", function (event, row) {
      d3.select(this).attr("opacity", 0.78);
      tooltip
        .style("opacity", 1)
        .html(`<strong>${row.__x}</strong><b>${d3.format(",")(row.__y)}</b>`);
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
    .call(d3.axisBottom(x).tickSizeOuter(0))
    .selectAll("text")
    .attr("text-anchor", rows.length > 12 ? "end" : "middle")
    .attr("transform", rows.length > 12 ? "rotate(-35)" : null)
    .attr("dx", rows.length > 12 ? "-0.6em" : null)
    .attr("dy", rows.length > 12 ? "0.2em" : "0.8em");

  plot.append("g").attr("class", "axis").call(d3.axisLeft(y).ticks(5).tickFormat(d3.format(".2s")));
}

watch(() => [props.data, props.xKey, props.yKey, props.color], render, { deep: true });

onMounted(() => {
  render();
  resizeObserver = new ResizeObserver(render);
  resizeObserver.observe(root.value);
});

onBeforeUnmount(() => resizeObserver?.disconnect());
</script>
