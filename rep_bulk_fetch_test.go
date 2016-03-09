package benchmark_bbs_test

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/cloudfoundry-incubator/benchmark-bbs/reporter"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-golang/operationq"
)

const (
	RepBulkFetching   = "RepBulkFetching"
	RepBulkLoop       = "RepBulkLoop"
	RepClaimActualLRP = "RepClaimActualLRP"
	RepStartActualLRP = "RepStartActualLRP"
)

var repBulkCycle = 30

func printProgress(done *int32, total int32) {
	for {
		fmt.Print("\r")
		fmt.Print("[")
		current := atomic.LoadInt32(done)
		done := int32(float64(current) / float64(total) * 100)
		for i := int32(0); i < done; i++ {
			fmt.Print("=")
		}
		for i := done; i < 100; i++ {
			fmt.Print(" ")
		}
		fmt.Printf("] %d/%d", current, total)
		if done == 100 {
			fmt.Println("")
			return
		}
		time.Sleep(1 * time.Second)
	}
}

var BenchmarkRepFetching = func(numReps, numTrials int) {
	FDescribe("Fetching for rep bulk loop", func() {
		Measure("data for rep bulk", func(b Benchmarker) {
			totalRan := int32(0)
			totalQueued := int32(0)
			var err error
			wg := sync.WaitGroup{}
			queue := operationq.NewSlidingQueue(numTrials)
			Expect(err).NotTo(HaveOccurred())
			totalThings := numReps * numTrials
			totalDone := int32(0)

			go printProgress(&totalDone, int32(totalThings))

			for i := 0; i < numReps; i++ {
				cellID := fmt.Sprintf("cell-%d", i)
				wg.Add(1)

				go func(cellID string) {
					defer wg.Done()

					for j := 0; j < numTrials; j++ {
						sleepDuration := 30 * time.Second
						if j == 0 {
							numMilli := rand.Intn(30000)
							sleepDuration = time.Duration(numMilli) * time.Millisecond
						}
						time.Sleep(sleepDuration)

						b.Time("rep bulk loop", func() {
							defer atomic.AddInt32(&totalDone, 1)
							defer GinkgoRecover()
							var actuals []*models.ActualLRPGroup
							b.Time("rep bulk fetch", func() {
								actuals, err = bbsClient.ActualLRPGroups(models.ActualLRPFilter{CellID: cellID})
								Expect(err).NotTo(HaveOccurred())
							}, reporter.ReporterInfo{
								MetricName: RepBulkFetching,
							})

							expectedActualLRPCount, ok := expectedActualLRPCounts[cellID]
							Expect(ok).To(BeTrue())

							expectedActualLRPVariation, ok := expectedActualLRPVariations[cellID]
							Expect(ok).To(BeTrue())

							Expect(len(actuals)).To(BeNumerically("~", expectedActualLRPCount, expectedActualLRPVariation))

							numActuals := len(actuals)
							for k := 0; k < numActuals; k++ {
								actualLRP, _ := actuals[k].Resolve()
								atomic.AddInt32(&totalQueued, 1)
								queue.Push(&lrpOperation{actualLRP, percentWrites, b, &totalRan})
							}
						}, reporter.ReporterInfo{
							MetricName: RepBulkLoop,
						})
					}
				}(cellID)
			}
			wg.Wait()

			time.Sleep(1 * time.Second)
			fmt.Println("Done with all loops, waiting for queue to clear out...")
			go printProgress(&totalRan, int32(totalQueued))
			Eventually(func() int32 { return totalRan }, 10*time.Minute).Should(Equal(totalQueued), "should have run the same number of queued LRP operations")
		}, 1)
	})
}

type lrpOperation struct {
	actualLRP     *models.ActualLRP
	percentWrites float64
	b             Benchmarker
	globalCount   *int32
}

func (lo *lrpOperation) Key() string {
	return lo.actualLRP.ProcessGuid
}

func (lo *lrpOperation) Execute() {
	defer GinkgoRecover()
	defer atomic.AddInt32(lo.globalCount, 1)
	randomNum := rand.Float64() * 100.0

	// divided by 2 because the start following the claim cause two writes.
	p := (lo.percentWrites / 2)

	isClaiming := randomNum < p
	actualLRP := lo.actualLRP

	lo.b.Time("start actual LRP", func() {
		netInfo := models.NewActualLRPNetInfo("1.2.3.4", models.NewPortMapping(61999, 8080))
		err := bbsClient.StartActualLRP(&actualLRP.ActualLRPKey, &actualLRP.ActualLRPInstanceKey, &netInfo)
		Expect(err).NotTo(HaveOccurred())
	}, reporter.ReporterInfo{
		MetricName: RepStartActualLRP,
	})

	if isClaiming {
		lo.b.Time("claim actual LRP", func() {
			index := int(actualLRP.ActualLRPKey.Index)
			err := bbsClient.ClaimActualLRP(actualLRP.ActualLRPKey.ProcessGuid, index, &actualLRP.ActualLRPInstanceKey)
			Expect(err).NotTo(HaveOccurred())
		}, reporter.ReporterInfo{
			MetricName: RepClaimActualLRP,
		})
	}
}
