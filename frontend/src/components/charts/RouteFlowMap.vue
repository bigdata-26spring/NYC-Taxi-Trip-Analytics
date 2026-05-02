<template>
  <div ref="root" class="chart-shell map-shell">
    <EmptyState v-if="!routes.length" title="No route flow data" />
    <div class="map-controls" v-if="routes.length">
      <button type="button" title="Zoom in" @click="zoomBy(1.35)">+</button>
      <button type="button" title="Zoom out" @click="zoomBy(0.75)">-</button>
      <button type="button" title="Reset map" @click="resetZoom">Reset</button>
    </div>
  </div>
</template>

<script setup>
import * as d3 from "d3";
import { onBeforeUnmount, onMounted, ref, watch } from "vue";

import EmptyState from "./EmptyState.vue";

const props = defineProps({
  routes: {
    type: Array,
    default: () => [],
  },
  limit: {
    type: Number,
    default: 35,
  },
  height: {
    type: Number,
    default: 560,
  },
});

const emit = defineEmits(["select-route"]);

const root = ref(null);
let resizeObserver;
let geojson = null;
let svgSelection = null;
let zoomBehavior = null;

async function loadGeojson() {
  if (geojson) {
    return geojson;
  }

  const response = await fetch("/data/taxi_zones.geojson");
  if (!response.ok) {
    throw new Error("taxi_zones.geojson is missing");
  }

  geojson = await response.json();
  return geojson;
}

function arcPath(source, target) {
  const dx = target[0] - source[0];
  const dy = target[1] - source[1];
  const distance = Math.sqrt(dx * dx + dy * dy) || 1;
  const lift = Math.min(90, distance * 0.32);
  const mx = (source[0] + target[0]) / 2;
  const my = (source[1] + target[1]) / 2;
  const cx = mx - (dy / distance) * lift;
  const cy = my + (dx / distance) * lift;

  return `M${source[0]},${source[1]} Q${cx},${cy} ${target[0]},${target[1]}`;
}

async function render() {
  if (!root.value || !props.routes.length) {
    return;
  }

  const zones = await loadGeojson();
  const width = Math.max(360, root.value.clientWidth || 820);
  const height = props.height;
  const margin = 12;
  const projection = d3.geoMercator().fitExtent(
    [
      [margin, margin],
      [width - margin, height - margin],
    ],
    zones
  );
  const path = d3.geoPath(projection);
  const centroids = new Map(zones.features.map((feature) => [Number(feature.properties.LocationID), path.centroid(feature)]));
  const rows = props.routes
    .filter((route) => centroids.has(Number(route.PULocationID)) && centroids.has(Number(route.DOLocationID)))
    .slice(0, props.limit);
  const maxTrips = d3.max(rows, (route) => Number(route.trip_count)) || 1;
  const stroke = d3.scaleLinear().domain([0, maxTrips]).range([1.2, 8]);
  const color = d3
    .scaleSequential(d3.interpolateRgbBasis(["#21bfd0", "#f7c948", "#d44a5f"]))
    .domain([0, maxTrips]);

  d3.select(root.value).selectAll("svg").remove();
  d3.select(root.value).selectAll(".chart-tooltip").remove();

  const tooltip = d3.select(root.value).append("div").attr("class", "chart-tooltip");
  const svg = d3.select(root.value).append("svg").attr("viewBox", `0 0 ${width} ${height}`);
  const layer = svg.append("g");
  svgSelection = svg;
  zoomBehavior = d3
    .zoom()
    .scaleExtent([1, 8])
    .on("zoom", (event) => layer.attr("transform", event.transform));

  svg.call(zoomBehavior);

  layer
    .append("g")
    .selectAll("path")
    .data(zones.features)
    .join("path")
    .attr("d", path)
    .attr("fill", "#edf2f5")
    .attr("stroke", "#ffffff")
    .attr("stroke-width", 0.7);

  layer
    .append("g")
    .attr("class", "route-lines")
    .selectAll("path")
    .data(rows)
    .join("path")
    .attr("d", (route) => arcPath(centroids.get(Number(route.PULocationID)), centroids.get(Number(route.DOLocationID))))
    .attr("fill", "none")
    .attr("stroke", (route) => color(Number(route.trip_count)))
    .attr("stroke-width", (route) => stroke(Number(route.trip_count)))
    .attr("stroke-linecap", "round")
    .attr("opacity", 0.74)
    .style("cursor", "pointer")
    .on("mouseenter", function (event, route) {
      d3.select(this).attr("opacity", 1).attr("stroke-width", stroke(Number(route.trip_count)) + 2).raise();
      tooltip
        .style("opacity", 1)
        .html(
          `<strong>${route.route_name}</strong>
          <span>Rank ${route.route_rank || route.route_rank_in_hour || route.route_rank_in_year_month || "-"}</span>
          <b>${d3.format(",")(Number(route.trip_count))} trips</b>
          <span>$${d3.format(",.0f")(Number(route.total_revenue || 0))} revenue</span>`
        );
    })
    .on("mousemove", (event) => {
      tooltip.style("left", `${event.offsetX + 14}px`).style("top", `${event.offsetY + 14}px`);
    })
    .on("mouseleave", function (event, route) {
      d3.select(this).attr("opacity", 0.74).attr("stroke-width", stroke(Number(route.trip_count)));
      tooltip.style("opacity", 0);
    })
    .on("click", (event, route) => {
      emit("select-route", route);
    });

  const endpoints = rows.flatMap((route) => [
    { type: "pickup", point: centroids.get(Number(route.PULocationID)), route },
    { type: "dropoff", point: centroids.get(Number(route.DOLocationID)), route },
  ]);

  layer
    .append("g")
    .selectAll("circle")
    .data(endpoints)
    .join("circle")
    .attr("cx", (row) => row.point[0])
    .attr("cy", (row) => row.point[1])
    .attr("r", (row) => (row.type === "pickup" ? 3.4 : 2.4))
    .attr("fill", (row) => (row.type === "pickup" ? "#17202a" : "#ffffff"))
    .attr("stroke", "#17202a")
    .attr("stroke-width", 1);
}

function zoomBy(scale) {
  if (svgSelection && zoomBehavior) {
    svgSelection.transition().duration(260).call(zoomBehavior.scaleBy, scale);
  }
}

function resetZoom() {
  if (svgSelection && zoomBehavior) {
    svgSelection.transition().duration(280).call(zoomBehavior.transform, d3.zoomIdentity);
  }
}

watch(() => [props.routes, props.limit], () => render().catch(() => {}), { deep: true });

onMounted(() => {
  render().catch(() => {});
  resizeObserver = new ResizeObserver(() => render().catch(() => {}));
  resizeObserver.observe(root.value);
});

onBeforeUnmount(() => resizeObserver?.disconnect());
</script>
