"""Host ingress layer (task #209, issue #92).

Reads ``tcp_port`` allocation rows and materializes/removes port
forwarder processes so ``bridge reserve tcp_port`` is all a project
needs to publish a listener. Forward targets ride inside the
allocation ``purpose`` field as a ``fwd=<spec>`` token - see
docs/ALLOCATIONS.md for the grammar.
"""
