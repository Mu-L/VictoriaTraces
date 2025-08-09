---
weight: 2
title: Roadmap
menu:
  docs:
    identifier: vt-roadmap
    parent: victoriatraces
    weight: 2
    title: Roadmap
tags:
  - traces
aliases:
- /victoriatraces/roadmap.html
---

The following items need to be completed before general availability (GA) version:
- [ ] Finalize the data structure and commit to backward compatibility.
- [ ] Finalize the data distribution algorithm in the cluster version.
- [ ] Provide monitoring and alerting rules for VictoriaTraces single-node and cluster version.
- [ ] Provide alerting support on traces data with [vmalert](https://docs.victoriametrics.com/vmalert/).

The following functionality is planned in the future versions of VictoriaTraces after GA:
- [ ] Provide web UI to visualize traces.
- [ ] Provide [HTTP APIs](https://grafana.com/docs/tempo/latest/api_docs/) of Tempo Query-frontend.
- [ ] Support tail-based sampling/downsampling.

Refer to [the Roadmap of VictoriaLogs](https://docs.victoriametrics.com/victorialogs/roadmap/#) as well for information 
about object storage and retention filters. 