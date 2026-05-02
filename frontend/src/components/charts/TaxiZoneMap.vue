<template>
  <div ref="root" class="chart-shell map-shell">
    <EmptyState v-if="!metrics.length" title="No map metrics" message="Load top-zone data before drawing the map." />
    <div class="map-controls" v-if="metrics.length">
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
  metrics: {
    type: Array,
    default: () => [],
  },
  metricKey: {
    type: String,
    default: "total_trips",
  },
  metricLabel: {
    type: String,
    default: "trips",
  },
  idKey: {
    type: String,
    default: "PULocationID",
  },
  height: {
    type: Number,
    default: 520,
  },
});

const emit = defineEmits(["select-zone"]);

const root = ref(null);
let resizeObserver;
let geojson = null;
let svgSelection = null;
let zoomBehavior = null;

function featureId(feature) {
  return Number(feature.properties.LocationID);
}

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

async function render() {
  if (!root.value || !props.metrics.length) {
    return;
  }

  const zones = await loadGeojson();
  const width = Math.max(360, root.value.clientWidth || 760);
  const height = props.height;
  const margin = 12;
  const values = new Map(
    props.metrics.map((row) => [Number(row[props.idKey]), Number(row[props.metricKey] || 0)])
  );
  const rowsById = new Map(props.metrics.map((row) => [Number(row[props.idKey]), row]));
  const maxValue = d3.max([...values.values()]) || 1;
  const color = d3
    .scaleSequential(d3.interpolateRgbBasis(["#f0f2f4", "#f7c948", "#d44a5f"]))
    .domain([0, maxValue]);
  const projection = d3.geoMercator().fitExtent(
    [
      [margin, margin],
      [width - margin, height - margin],
    ],
    zones
  );
  const path = d3.geoPath(projection);

  d3.select(root.value).selectAll("svg").remove();
  d3.select(root.value).selectAll(".chart-tooltip").remove();

  const tooltip = d3.select(root.value).append("div").attr("class", "chart-tooltip");
  const svg = d3
    .select(root.value)
    .append("svg")
    .attr("viewBox", `0 0 ${width} ${height}`)
    .attr("role", "img");
  const mapLayer = svg.append("g");
  svgSelection = svg;
  zoomBehavior = d3
    .zoom()
    .scaleExtent([1, 8])
    .on("zoom", (event) => {
      mapLayer.attr("transform", event.transform);
    });

  svg.call(zoomBehavior);

  mapLayer
    .selectAll("path")
    .data(zones.features)
    .join("path")
    .attr("class", "zone-path")
    .attr("d", path)
    .attr("fill", (feature) => {
      const value = values.get(featureId(feature));
      return value ? color(value) : "#eef2f5";
    })
    .attr("stroke", "#ffffff")
    .attr("stroke-width", 0.7)
    .style("cursor", (feature) => (values.has(featureId(feature)) ? "pointer" : "default"))
    .on("mouseenter", function (event, feature) {
      const value = values.get(featureId(feature)) || 0;
      const row = rowsById.get(featureId(feature)) || {};
      d3.select(this).attr("stroke", "#17202a").attr("stroke-width", 1.6).raise();
      tooltip
        .style("opacity", 1)
        .html(
          `<strong>${feature.properties.zone}</strong>
          <span>${feature.properties.borough}</span>
          <b>${d3.format(",.2f")(value)} ${props.metricLabel}</b>
          ${row.zone_rank_in_year ? `<span>Rank ${row.zone_rank_in_year}</span>` : ""}`
        );
    })
    .on("mousemove", (event) => {
      tooltip.style("left", `${event.offsetX + 14}px`).style("top", `${event.offsetY + 14}px`);
    })
    .on("mouseleave", function () {
      d3.select(this).attr("stroke", "#ffffff").attr("stroke-width", 0.7);
      tooltip.style("opacity", 0);
    })
    .on("click", (event, feature) => {
      const id = featureId(feature);
      emit("select-zone", {
        LocationID: id,
        feature,
        metricValue: values.get(id) || 0,
        row: rowsById.get(id) || null,
      });
    })
    .append("title")
    .text((feature) => {
      const value = values.get(featureId(feature)) || 0;
      return `${feature.properties.zone}: ${d3.format(",")(value)}`;
    });

  const legendWidth = 180;
  const legendHeight = 8;
  const legendX = width - legendWidth - 24;
  const legendY = height - 30;
  const defs = svg.append("defs");
  const gradient = defs.append("linearGradient").attr("id", "zoneMapGradient");

  d3.range(0, 1.01, 0.1).forEach((stop) => {
    gradient
      .append("stop")
      .attr("offset", `${stop * 100}%`)
      .attr("stop-color", color(stop * maxValue));
  });

  svg
    .append("rect")
    .attr("x", legendX)
    .attr("y", legendY)
    .attr("width", legendWidth)
    .attr("height", legendHeight)
    .attr("rx", 4)
    .attr("fill", "url(#zoneMapGradient)");

  svg
    .append("text")
    .attr("class", "legend-label")
    .attr("x", legendX)
    .attr("y", legendY - 7)
    .text(props.metricLabel);

  svg
    .append("text")
    .attr("class", "legend-label")
    .attr("text-anchor", "end")
    .attr("x", legendX + legendWidth)
    .attr("y", legendY + 24)
    .text(d3.format(".2s")(maxValue));
}

function zoomBy(scale) {
  if (!svgSelection || !zoomBehavior) {
    return;
  }

  svgSelection.transition().duration(260).call(zoomBehavior.scaleBy, scale);
}

function resetZoom() {
  if (!svgSelection || !zoomBehavior) {
    return;
  }

  svgSelection.transition().duration(280).call(zoomBehavior.transform, d3.zoomIdentity);
}

watch(() => [props.metrics, props.metricKey, props.metricLabel, props.idKey], render, { deep: true });

onMounted(() => {
  render().catch(() => {});
  resizeObserver = new ResizeObserver(() => render().catch(() => {}));
  resizeObserver.observe(root.value);
});

onBeforeUnmount(() => resizeObserver?.disconnect());
</script>
