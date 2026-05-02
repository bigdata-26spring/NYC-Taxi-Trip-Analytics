<template>
  <div class="empty-state">
    <strong>{{ title }}</strong>
    <span>{{ message }}</span>
  </div>
</template>

<script setup>
defineProps({
  title: {
    type: String,
    default: "No data",
  },
  message: {
    type: String,
    default: "Try changing filters or loading the API again.",
  },
});
</script>
