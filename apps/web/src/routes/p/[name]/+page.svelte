<script lang="ts">
  // The playlist deep link. The payload rides the URL FRAGMENT, so it never reaches the server —
  // there is nothing to store and nothing to log. Ingest happens locally, then we land on the
  // playlist's own route.
  import { PlaylistView } from "@trackerstream/ui";
</script>

<PlaylistView />
