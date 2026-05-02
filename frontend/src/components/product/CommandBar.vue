<template>
  <header class="command-bar">
    <div>
      <p class="eyebrow">{{ eyebrow }}</p>
      <h2>{{ title }}</h2>
    </div>

    <div class="command-controls">
      <label>
        <span>Year</span>
        <select :value="year" @change="$emit('update:year', normalizedYear($event.target.value))">
          <option :value="null">All</option>
          <option v-for="item in years" :key="item" :value="item">{{ item }}</option>
        </select>
      </label>

      <label>
        <span>Borough</span>
        <select :value="borough" @change="$emit('update:borough', $event.target.value)">
          <option value="">All</option>
          <option v-for="item in boroughs" :key="item" :value="item">{{ item }}</option>
        </select>
      </label>

      <label>
        <span>Hour</span>
        <select :value="hour" @change="$emit('update:hour', Number($event.target.value))">
          <option v-for="item in hours" :key="item" :value="item">
            {{ String(item).padStart(2, "0") }}:00
          </option>
        </select>
      </label>

      <button class="icon-button command-refresh" type="button" title="Reload data" @click="$emit('reload')">
        <RefreshCw :size="18" />
      </button>
    </div>
  </header>
</template>

<script setup>
import { RefreshCw } from "lucide-vue-next";

defineProps({
  eyebrow: {
    type: String,
    default: "MongoDB Atlas + FastAPI + Vue/D3",
  },
  title: {
    type: String,
    required: true,
  },
  years: {
    type: Array,
    default: () => [],
  },
  boroughs: {
    type: Array,
    default: () => [],
  },
  hours: {
    type: Array,
    default: () => [],
  },
  year: {
    type: Number,
    default: null,
  },
  borough: {
    type: String,
    default: "",
  },
  hour: {
    type: Number,
    default: 18,
  },
});

defineEmits(["update:year", "update:borough", "update:hour", "reload"]);

function normalizedYear(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) && value !== "" ? numeric : null;
}
</script>
