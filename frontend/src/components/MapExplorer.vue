<template>
  <section class="map-explorer">
    <div class="map-toolbar">
      <div class="segmented-control" aria-label="Map mode">
        <button
          v-for="mode in mapModes"
          :key="mode.key"
          type="button"
          :class="{ active: activeMode === mode.key }"
          @click="activeMode = mode.key"
        >
          <component :is="mode.icon" :size="15" />
          <span>{{ mode.label }}</span>
        </button>
      </div>

      <div class="map-toolbar__meta">
        <span>{{ selectedYear || "All years" }}</span>
        <span>{{ selectedBorough || "All boroughs" }}</span>
        <span>{{ String(selectedHour).padStart(2, "0") }}:00</span>
      </div>
    </div>

    <div v-if="localError" class="error-banner map-error">{{ localError }}</div>
    <div v-if="loading" class="loading-bar" />

    <section class="map-layout">
      <div class="map-stage">
        <TaxiZoneMap
          v-if="activeMode !== 'flow'"
          :metrics="zoneMapMetrics"
          :metric-key="zoneMetricKey"
          :metric-label="zoneMetricLabel"
          :height="620"
          @select-zone="selectZone"
        />
        <RouteFlowMap
          v-else
          :routes="flowRows"
          :limit="80"
          :height="620"
          @select-route="selectRoute"
        />
      </div>

      <aside class="detail-panel">
        <header>
          <p class="eyebrow">{{ detailEyebrow }}</p>
          <h3>{{ detailTitle }}</h3>
        </header>

        <div class="detail-stat-grid">
          <div v-for="stat in detailStats" :key="stat.label" class="detail-stat">
            <span>{{ stat.label }}</span>
            <strong>{{ stat.value }}</strong>
          </div>
        </div>

        <div v-if="selectedZoneProfile?.top_dropoff_zone" class="detail-callout">
          <span>Top destination</span>
          <strong>{{ selectedZoneProfile.top_dropoff_zone }}</strong>
          <p>{{ integer(selectedZoneProfile.top_dropoff_trip_count) }} trips</p>
        </div>

        <div v-if="selectedRouteProfile?.route_name" class="detail-callout">
          <span>Peak route hour</span>
          <strong>{{ formatHour(selectedRouteProfile.peak_hour) }}</strong>
          <p>{{ integer(selectedRouteProfile.peak_hour_trip_count) }} trips</p>
        </div>

        <div class="detail-note">
          <span v-if="activeMode === 'flow'">
            Flow lines use zone centroids, so geometry is an estimated OD connection rather than GPS traces.
          </span>
          <span v-else>Click a taxi zone to pin its profile here.</span>
        </div>
      </aside>
    </section>

    <section class="dashboard-grid compact-grid">
      <ChartPanel title="Top Zone Profiles" eyebrow="Map detail">
        <HorizontalBarChart
          :data="zoneProfiles"
          label-key="pickup_zone"
          value-key="total_trips"
          color="#0f1720"
          :limit="10"
          :height="310"
        />
      </ChartPanel>

      <ChartPanel title="OD Flow Ranking" eyebrow="Selected hour">
        <DataTable :rows="flowRows" :columns="flowColumns" :limit="10" />
      </ChartPanel>

      <ChartPanel class="span-2" title="Route Concentration" eyebrow="Pareto curve">
        <LineChart
          :data="routeConcentration"
          x-key="route_order"
          y-key="cumulative_trip_share"
          :height="300"
        />
      </ChartPanel>
    </section>
  </section>
</template>

<script setup>
import { Activity, MapPinned, Route, Target } from "lucide-vue-next";
import { computed, onMounted, ref, watch } from "vue";

import { api } from "@/api/client";
import DataTable from "@/components/charts/DataTable.vue";
import HorizontalBarChart from "@/components/charts/HorizontalBarChart.vue";
import LineChart from "@/components/charts/LineChart.vue";
import RouteFlowMap from "@/components/charts/RouteFlowMap.vue";
import TaxiZoneMap from "@/components/charts/TaxiZoneMap.vue";
import ChartPanel from "@/components/layout/ChartPanel.vue";
import { compact, decimal, integer, money, pct } from "@/utils/format";

const props = defineProps({
  selectedYear: {
    type: Number,
    default: null,
  },
  selectedBorough: {
    type: String,
    default: "",
  },
  selectedHour: {
    type: Number,
    default: 18,
  },
  forecastZones: {
    type: Array,
    default: () => [],
  },
  refreshKey: {
    type: Number,
    default: 0,
  },
});

const mapModes = [
  { key: "pickup", label: "Pickup", icon: MapPinned },
  { key: "forecast", label: "Forecast Error", icon: Target },
  { key: "flow", label: "OD Flow", icon: Route },
  { key: "profile", label: "Zone Profile", icon: Activity },
];

const activeMode = ref("pickup");
const loading = ref(false);
const localError = ref("");
const zoneProfiles = ref([]);
const routeProfiles = ref([]);
const flowHour = ref([]);
const flowMonth = ref([]);
const routeConcentration = ref([]);
const selectedZoneProfile = ref(null);
const selectedRouteProfile = ref(null);

const flowRows = computed(() => {
  const rows = flowHour.value.length ? flowHour.value : flowMonth.value;
  return rows.filter((row) => {
    if (!props.selectedBorough) {
      return true;
    }
    return row.pickup_borough === props.selectedBorough || row.dropoff_borough === props.selectedBorough;
  });
});

const zoneMapMetrics = computed(() => {
  if (activeMode.value === "forecast") {
    return props.forecastZones;
  }
  return zoneProfiles.value;
});

const zoneMetricKey = computed(() => {
  if (activeMode.value === "forecast") {
    return "aggregate_absolute_error";
  }
  return "total_trips";
});

const zoneMetricLabel = computed(() => {
  if (activeMode.value === "forecast") {
    return "absolute error";
  }
  return "trips";
});

const selectedDetail = computed(() => selectedRouteProfile.value || selectedZoneProfile.value || zoneProfiles.value[0] || {});
const detailEyebrow = computed(() => (selectedRouteProfile.value ? "Route profile" : "Zone profile"));
const detailTitle = computed(() => selectedDetail.value.route_name || selectedDetail.value.pickup_zone || "Select a zone");
const detailStats = computed(() => {
  const row = selectedDetail.value;
  if (selectedRouteProfile.value) {
    return [
      { label: "Trips", value: compact(row.trip_count) },
      { label: "Revenue", value: money(row.total_revenue) },
      { label: "Avg fare", value: money(row.avg_fare_amount) },
      { label: "Tip share", value: pct(row.avg_tip_share_of_total) },
      { label: "Avg miles", value: decimal(row.avg_trip_distance, 2) },
      { label: "Avg min", value: decimal(row.avg_trip_duration_min, 1) },
    ];
  }

  return [
    { label: "Trips", value: compact(row.total_trips) },
    { label: "Revenue", value: money(row.total_revenue) },
    { label: "Avg / day", value: decimal(row.avg_trips_per_active_day, 1) },
    { label: "Credit card", value: pct(row.credit_card_share) },
    { label: "Peak hour", value: formatHour(row.peak_hour) },
    { label: "Avg fare", value: money(row.avg_fare_amount) },
  ];
});

const flowColumns = [
  {
    key: "route_rank_in_hour",
    label: "Rank",
    numeric: true,
    format: (value, row) => integer(value || row.route_rank_in_year_month || row.route_rank),
  },
  { key: "route_name", label: "Route" },
  { key: "trip_count", label: "Trips", numeric: true, format: integer },
  { key: "avg_trip_duration_min", label: "Min", numeric: true, format: (value) => decimal(value, 1) },
  { key: "avg_revenue_per_trip", label: "Rev / Trip", numeric: true, format: money },
];

function formatHour(value) {
  const hour = Number(value);
  if (!Number.isFinite(hour)) {
    return "-";
  }
  return `${String(hour).padStart(2, "0")}:00`;
}

async function loadExplorerData() {
  loading.value = true;
  localError.value = "";

  try {
    const [
      zoneResult,
      routeResult,
      flowHourResult,
      flowMonthResult,
      concentrationResult,
    ] = await Promise.all([
      api.zoneProfiles({ borough: props.selectedBorough || null, limit: 300 }),
      api.routeProfiles({ pickup_borough: props.selectedBorough || null, limit: 300 }),
      api.mapOdFlowHour({ hour: props.selectedHour, borough: props.selectedBorough || null, limit: 500 }),
      api.mapOdFlowYearMonth({
        year: props.selectedYear,
        borough: props.selectedBorough || null,
        limit: 500,
      }),
      api.routeConcentration({ limit: 500 }),
    ]);

    zoneProfiles.value = zoneResult.data || [];
    routeProfiles.value = routeResult.data || [];
    flowHour.value = flowHourResult.data || [];
    flowMonth.value = flowMonthResult.data || [];
    routeConcentration.value = concentrationResult.data || [];
    selectedZoneProfile.value = zoneProfiles.value[0] || null;
    selectedRouteProfile.value = null;
  } catch (caught) {
    localError.value = caught.message || "Failed to load map explorer data.";
  } finally {
    loading.value = false;
  }
}

async function selectZone(selection) {
  selectedRouteProfile.value = null;
  const id = selection?.LocationID || selection?.row?.PULocationID;
  const fallback = selection?.row || zoneProfiles.value.find((row) => Number(row.PULocationID) === Number(id));
  selectedZoneProfile.value = fallback || selectedZoneProfile.value;

  if (!id) {
    return;
  }

  try {
    const result = await api.zoneProfile(id);
    selectedZoneProfile.value = result.data && Object.keys(result.data).length ? result.data : fallback;
  } catch {
    selectedZoneProfile.value = fallback;
  }
}

async function selectRoute(route) {
  selectedZoneProfile.value = null;
  selectedRouteProfile.value = route;

  if (!route?.PULocationID || !route?.DOLocationID) {
    return;
  }

  try {
    const result = await api.routeProfile(route.PULocationID, route.DOLocationID);
    selectedRouteProfile.value = result.data && Object.keys(result.data).length ? result.data : route;
  } catch {
    selectedRouteProfile.value = route;
  }
}

watch(() => [props.selectedYear, props.selectedBorough, props.selectedHour, props.refreshKey], loadExplorerData);

onMounted(loadExplorerData);
</script>
