context("Testing if binary data is equal to test data. Currently only works on my machine.")

test_that("Check if binary data is equal to test data",
{
  binFile<-"c:/MERRA/filejava/"
  mb<-MERRABin$new(file=binFile,endian="big")
  MERRA_Test<-test_data
  data("MERRA_Test")
  positions<-unique(MERRA_Test$pos)

  for(i in 1:length(positions)){
	  ts_bin<-(mb$readTS(positions[i]))
	  ts_tibble<-MERRA_Test %>% filter(pos==positions[i]) %>% dplyr::select(val)

	  tt<-tibble(bin=ts_bin,ncdf=ts_tibble$val) %>% mutate(diff=bin-ncdf)
	  #plot(tt$diff,main=i)
	  expect_equal(sum(round(tt$diff)),0)
   }

}

)
