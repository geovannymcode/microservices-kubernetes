// Load for the HPA demo: GET /inventories through the ClusterIP Service, as Order would call it.
import http from 'k6/http';
import { check } from 'k6';

const BASE = 'http://service-inventory.geovannycode.svc.cluster.local/services-inventory';

export default function () {
  const response = http.get(`${BASE}/inventories`, { headers: { Accept: 'application/json' } });
  check(response, { 'status 200': (r) => r.status === 200 });
}
