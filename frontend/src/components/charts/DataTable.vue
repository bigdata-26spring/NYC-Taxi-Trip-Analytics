<template>
  <div class="table-shell">
    <table>
      <thead>
        <tr>
          <th v-for="column in columns" :key="column.key">{{ column.label }}</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="(row, index) in rows.slice(0, limit)" :key="index">
          <td v-for="column in columns" :key="column.key" :class="{ numeric: column.numeric }">
            {{ column.format ? column.format(row[column.key], row) : row[column.key] }}
          </td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script setup>
defineProps({
  rows: {
    type: Array,
    default: () => [],
  },
  columns: {
    type: Array,
    default: () => [],
  },
  limit: {
    type: Number,
    default: 12,
  },
});
</script>
