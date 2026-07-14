// Thin re-export so the rest of the package doesn't care which entry point the `ipns` package
// exposes these on. Kept in one place because getting the ROUTING KEY wrong is a silent failure:
// validation would simply never match, and playlists would look like they "just don't sync".
export { multihashToIPNSRoutingKey, createIPNSRecord, marshalIPNSRecord, unmarshalIPNSRecord } from "ipns";
export { ipnsValidator } from "ipns/validator";
