<template>
  <section class="insight-banner">
    <div class="insight-banner__media" />
    <div class="insight-banner__copy">
      <p class="eyebrow">{{ eyebrow }}</p>
      <h2>{{ title }}</h2>
      <p>{{ summary }}</p>
    </div>
    <div class="insight-banner__stats">
      <div v-for="item in stats" :key="item.label">
        <span>{{ item.label }}</span>
        <strong>{{ item.value }}</strong>
      </div>
    </div>
  </section>
</template>

<script setup>
defineProps({
  eyebrow: {
    type: String,
    default: "Live analysis workspace",
  },
  title: {
    type: String,
    required: true,
  },
  summary: {
    type: String,
    required: true,
  },
  stats: {
    type: Array,
    default: () => [],
  },
});
</script>
