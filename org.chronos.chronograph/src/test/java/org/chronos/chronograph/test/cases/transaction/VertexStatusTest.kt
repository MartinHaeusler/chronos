package org.chronos.chronograph.test.cases.transaction

import org.apache.tinkerpop.gremlin.structure.T
import org.chronos.chronodb.inmemory.InMemoryChronoDB
import org.chronos.chronograph.api.transaction.VertexStatus
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest
import org.junit.Test
import strikt.api.expect
import strikt.assertions.isA
import strikt.assertions.isEqualTo

class VertexStatusTest : AllChronoGraphBackendsTest() {

    @Test
    fun canGetVertexStatus() {
        val g = this.graph

        val commit1 = g.tx().createThreadedTx().use { txGraph ->
            txGraph.addVertex(T.id, "a")
            txGraph.addVertex(T.id, "b")
            txGraph.addVertex(T.id, "c")
            txGraph.tx().commitAndReturnTimestamp()
        }

        val commit2 = g.tx().createThreadedTx().use { txGraph ->
            val a = txGraph.vertex("a")
            a.property("hello", "world")

            val c = txGraph.vertex("c")
            c.remove()

            txGraph.tx().commitAndReturnTimestamp()
        }

        g.tx().createThreadedTx().use { txGraph ->
            expect {
                that(txGraph.vertexStatus("a")).isA<VertexStatus.Exists>().and {
                    get { this.vertex().id() }.isEqualTo("a")
                    get { this.lastModificationTimestamp() }.isEqualTo(commit2)
                }

                that(txGraph.vertexStatus("b")).isA<VertexStatus.Exists>().and {
                    get { this.vertex().id() }.isEqualTo("b")
                    get { this.lastModificationTimestamp() }.isEqualTo(commit1)
                }

                that(txGraph.vertexStatus("c")).isA<VertexStatus.Deleted>().and {
                    get { this.lastModificationTimestamp() }.isEqualTo(commit2)
                }

                that(txGraph.vertexStatus("x")).isA<VertexStatus.NotFound>()
            }
        }
    }

    @Test
    @DontRunWithBackend(InMemoryChronoDB.BACKEND_NAME)
    fun canGetVertexStatusAfterRollover() {
        val g = this.graph

        val commit1 = g.tx().createThreadedTx().use { txGraph ->
            txGraph.addVertex(T.id, "a")
            txGraph.addVertex(T.id, "b")
            txGraph.addVertex(T.id, "c")
            txGraph.tx().commitAndReturnTimestamp()
        }

        val commit2 = g.tx().createThreadedTx().use { txGraph ->
            val a = txGraph.vertex("a")
            a.property("hello", "world")

            val c = txGraph.vertex("c")
            c.remove()

            txGraph.tx().commitAndReturnTimestamp()
        }

        g.maintenanceManager.performRolloverOnMaster()

        g.tx().createThreadedTx().use { txGraph ->
            expect {
                that(txGraph.vertexStatus("a")).describedAs("vertexStatus('a')")
                    .isA<VertexStatus.Exists>().and {
                        get { this.vertex().id() }.isEqualTo("a")
                        get { this.lastModificationTimestamp() }.isEqualTo(commit2)
                    }

                that(txGraph.vertexStatus("b")).describedAs("vertexStatus('b')")
                    .isA<VertexStatus.Exists>().and {
                        get { this.vertex().id() }.isEqualTo("b")
                        get { this.lastModificationTimestamp() }.isEqualTo(commit1)
                    }

                that(txGraph.vertexStatus("c")).describedAs("vertexStatus('c')")
                    .isA<VertexStatus.Deleted>().and {
                        get { this.lastModificationTimestamp() }.isEqualTo(commit2)
                    }

                that(txGraph.vertexStatus("x")).isA<VertexStatus.NotFound>()
            }
        }
    }

    @Test
    fun canGetVertexStatusAcrossBranches(){
        val g = this.graph

        val commit1 = g.tx().createThreadedTx().use { txGraph ->
            txGraph.addVertex(T.id, "a")
            txGraph.addVertex(T.id, "b")
            txGraph.addVertex(T.id, "c")
            txGraph.tx().commitAndReturnTimestamp()
        }

        g.branchManager.createBranch("sub")

        val commit2 = g.tx().createThreadedTx("sub").use { txGraph ->
            val a = txGraph.vertex("a")
            a.property("hello", "world")

            val c = txGraph.vertex("c")
            c.remove()

            txGraph.tx().commitAndReturnTimestamp()
        }

        g.tx().createThreadedTx("sub").use { txGraph ->
            expect {
                that(txGraph.vertexStatus("a")).describedAs("vertexStatus('a')")
                    .isA<VertexStatus.Exists>().and {
                        get { this.vertex().id() }.isEqualTo("a")
                        get { this.lastModificationTimestamp() }.isEqualTo(commit2)
                    }

                that(txGraph.vertexStatus("b")).describedAs("vertexStatus('b')")
                    .isA<VertexStatus.Exists>().and {
                        get { this.vertex().id() }.isEqualTo("b")
                        get { this.lastModificationTimestamp() }.isEqualTo(commit1)
                    }

                that(txGraph.vertexStatus("c")).describedAs("vertexStatus('c')")
                    .isA<VertexStatus.Deleted>().and {
                        get { this.lastModificationTimestamp() }.isEqualTo(commit2)
                    }

                that(txGraph.vertexStatus("x")).isA<VertexStatus.NotFound>()
            }
        }
    }


}
