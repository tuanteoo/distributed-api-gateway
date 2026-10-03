import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    vus: 50,
    duration: '10s',
};

export default function () {
    const res = http.get('http://host.docker.internal:8080/dummy/posts/1');

    check(res, {
        'is status 200 (Allowed)': (r) => r.status === 200,
        'is status 429 (Rate Limited)': (r) => r.status === 429,
        'is status 5xx (Server Error)': (r) => r.status >= 500,
    });

    sleep(0.01);
}