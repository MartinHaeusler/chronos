package org.chronos.chronograph.test.cases.transaction

import org.apache.tinkerpop.gremlin.structure.T
import org.chronos.chronodb.inmemory.InMemoryChronoDB
import org.chronos.chronograph.api.transaction.EdgeStatus
import org.chronos.chronograph.api.transaction.VertexStatus
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest
import org.junit.Test
import strikt.api.expect
import strikt.assertions.isA
import strikt.assertions.isEqualTo

class EdgeStatusTest : AllChronoGraphBackendsTest() {

    @Test
    fun canGetEdgeStatus() {
        val g = this.graph

        val commit1 = g.tx().createThreadedTx().use { txGraph ->
            val a = txGraph.addVertex(T.id, "a")
            val b = txGraph.addVertex(T.id, "b")
            val c = txGraph.addVertex(T.id, "c")
            val d = txGraph.addVertex(T.id, "d")

            a.addEdge("knows", b, T.id, "a->b")
            b.addEdge("knows", c, T.id, "b->c")
            c.addEdge("knows", d, T.id, "c->d")

            txGraph.tx().commitAndReturnTimestamp()
        }

        val commit2 = g.tx().createThreadedTx().use { txGraph ->
            val aToB = txGraph.edge("a->b")
            aToB.property("hello", "world")

            val cToD = txGraph.edge("c->d")
            cToD.remove()

            txGraph.tx().commitAndReturnTimestamp()
        }

        g.tx().createThreadedTx().use { txGraph ->
            expect {
                that(txGraph.edgeStatus("a->b")).isA<EdgeStatus.Exists>().and {
                    get { this.edge().id() }.isEqualTo("a->b")
                    get { this.lastModificationTimestamp() }.isEqualTo(commit2)
                }

                that(txGraph.edgeStatus("b->c")).isA<EdgeStatus.Exists>().and {
                    get { this.edge().id() }.isEqualTo("b->c")
                    get { this.lastModificationTimestamp() }.isEqualTo(commit1)
                }

                that(txGraph.edgeStatus("c->d")).isA<EdgeStatus.Deleted>().and {
                    get { this.lastModificationTimestamp() }.isEqualTo(commit2)
                }

                that(txGraph.edgeStatus("x->y")).isA<EdgeStatus.NotFound>()
            }
        }
    }

    @Test
    @DontRunWithBackend(InMemoryChronoDB.BACKEND_NAME)
    fun canGetEdgeStatusAfterRollover() {
        val g = this.graph

        val commit1 = g.tx().createThreadedTx().use { txGraph ->
            val a = txGraph.addVertex(T.id, "a")
            val b = txGraph.addVertex(T.id, "b")
            val c = txGraph.addVertex(T.id, "c")
            val d = txGraph.addVertex(T.id, "d")

            a.addEdge("knows", b, T.id, "a->b")
            b.addEdge("knows", c, T.id, "b->c")
            c.addEdge("knows", d, T.id, "c->d")

            txGraph.tx().commitAndReturnTimestamp()
        }

        val commit2 = g.tx().createThreadedTx().use { txGraph ->
            val a = txGraph.edge("a->b")
            a.property("hello", "world")

            val c = txGraph.edge("c->d")
            c.remove()

            txGraph.tx().commitAndReturnTimestamp()
        }

        g.maintenanceManager.performRolloverOnMaster()

        g.tx().createThreadedTx().use { txGraph ->
            expect {
                that(txGraph.edgeStatus("a->b")).describedAs("edgeStatus('a->b')")
                    .isA<EdgeStatus.Exists>().and {
                        get { this.edge().id() }.isEqualTo("a->b")
                        get { this.lastModificationTimestamp() }.isEqualTo(commit2)
                    }

                that(txGraph.edgeStatus("b->c")).describedAs("edgeStatus('b->c')")
                    .isA<EdgeStatus.Exists>().and {
                        get { this.edge().id() }.isEqualTo("b->c")
                        get { this.lastModificationTimestamp() }.isEqualTo(commit1)
                    }

                that(txGraph.edgeStatus("c->d")).describedAs("edgeStatus('c->d')")
                    .isA<EdgeStatus.Deleted>().and {
                        get { this.lastModificationTimestamp() }.isEqualTo(commit2)
                    }

                that(txGraph.vertexStatus("x->y")).isA<VertexStatus.NotFound>()
            }
        }
    }

    @Test
    fun canGetVertexStatusAcrossBranches() {
        val g = this.graph

        val commit1 = g.tx().createThreadedTx().use { txGraph ->
            val a = txGraph.addVertex(T.id, "a")
            val b = txGraph.addVertex(T.id, "b")
            val c = txGraph.addVertex(T.id, "c")
            val d = txGraph.addVertex(T.id, "d")

            a.addEdge("knows", b, T.id, "a->b")
            b.addEdge("knows", c, T.id, "b->c")
            c.addEdge("knows", d, T.id, "c->d")

            txGraph.tx().commitAndReturnTimestamp()
        }

        g.branchManager.createBranch("sub")

        val commit2 = g.tx().createThreadedTx("sub").use { txGraph ->
            val a = txGraph.edge("a->b")
            a.property("hello", "world")

            val c = txGraph.edge("c->d")
            c.remove()

            txGraph.tx().commitAndReturnTimestamp()
        }

        g.tx().createThreadedTx("sub").use { txGraph ->
            expect {
                that(txGraph.edgeStatus("a->b")).describedAs("edgeStatus('a->b')")
                    .isA<EdgeStatus.Exists>().and {
                        get { this.edge().id() }.isEqualTo("a->b")
                        get { this.lastModificationTimestamp() }.isEqualTo(commit2)
                    }

                that(txGraph.edgeStatus("b->c")).describedAs("edgeStatus('b->c')")
                    .isA<EdgeStatus.Exists>().and {
                        get { this.edge().id() }.isEqualTo("b->c")
                        get { this.lastModificationTimestamp() }.isEqualTo(commit1)
                    }

                that(txGraph.edgeStatus("c->d")).describedAs("edgeStatus('c->d')")
                    .isA<EdgeStatus.Deleted>().and {
                        get { this.lastModificationTimestamp() }.isEqualTo(commit2)
                    }

                that(txGraph.edgeStatus("x->y")).isA<EdgeStatus.NotFound>()
            }
        }
    }


}
