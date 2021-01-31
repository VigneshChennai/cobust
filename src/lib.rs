use std::future::Future;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, UnwindSafe};
use std::pin::Pin;

use futures::channel::mpsc::{channel, Receiver, SendError};
use futures::stream::TryStreamExt;
use futures::task::{Context, Poll};
use futures::StreamExt;
use futures::{FutureExt, SinkExt, Stream};
use thiserror::Error;

use retryiter::{IntoRetryIter, ItemStatus};
use std::ops::DerefMut;

#[derive(Error, Debug)]
enum InternalServiceError {
    #[error("Error while sending the result to the channel {0}")]
    ResultSendError(#[from] SendError),
    #[error("Function panicked")]
    Panicked,
}

pub trait IntoCobust<ITR, IVAL, IFN, IRVAL, FN, FUT, RVAL>
where
    IVAL: Clone,
    IRVAL: Clone,
    ITR: Iterator<Item = IVAL>,
    IFN: Fn() -> IRVAL,
    FN: Fn(AssertUnwindSafe<IRVAL>, IVAL) -> FUT,
    FUT: Future<Output = RVAL> + UnwindSafe,
{
    fn into_cobust(
        self,
        max_concurrency: NonZeroUsize,
        retries: usize,
        init_fn: IFN,
        task_fn: FN,
    ) -> CobustStream<RVAL, IVAL>;
}

impl<ITR, IVAL, IFN, IRVAL, FN, FUT, RVAL> IntoCobust<ITR, IVAL, IFN, IRVAL, FN, FUT, RVAL> for ITR
where
    RVAL: 'static,
    IVAL: Clone,
    IRVAL: Clone,
    ITR: 'static + Iterator<Item = IVAL>,
    IFN: 'static + Fn() -> IRVAL,
    FN: 'static + Fn(AssertUnwindSafe<IRVAL>, IVAL) -> FUT,
    FUT: Future<Output = RVAL> + UnwindSafe,
{
    fn into_cobust(
        self,
        max_concurrency: NonZeroUsize,
        retries: usize,
        init_fn: IFN,
        task_fn: FN,
    ) -> CobustStream<RVAL, IVAL> {
        let (sender, receiver) = channel::<RVAL>(max_concurrency.into());

        let cobust_task = Box::pin(async move {
            let irval = init_fn();
            let aus_irval = AssertUnwindSafe(irval);

            let mut iter = self.retries( retries);

            loop {
                let concurrent_task = futures::stream::iter(iter.deref_mut())
                    .map(|v| Ok((AssertUnwindSafe(aus_irval.0.clone()), v)))
                    .try_for_each_concurrent(max_concurrency.get(), |(iv, mut item)| {
                        let mut sender = sender.clone();
                        let task_fn = &task_fn;

                        let v = item.value();
                        item.set_default(ItemStatus::NotDone);

                        async move {
                            let result = task_fn(AssertUnwindSafe(iv.0.clone()), v.clone())
                                .catch_unwind()
                                .await;

                            match result {
                                Ok(v) => {
                                    item.succeeded();
                                    let result = sender.send(v).await;
                                    match result {
                                        Err(send_err) => {
                                            Err(InternalServiceError::ResultSendError(send_err))
                                        }
                                        _ => Ok(()),
                                    }
                                }
                                Err(_) => {
                                    // Panicked!!
                                    item.failed(None);
                                    Err(InternalServiceError::Panicked)
                                }
                            }
                        }
                    });

                match concurrent_task.await {
                    Ok(_) => break,
                    Err(InternalServiceError::Panicked) => {
                        continue;
                    }
                    Err(InternalServiceError::ResultSendError(_)) => {
                        println!("Channel send error!");
                        break;
                    }
                }
            }
            drop(sender);
            iter.failed_items()
        });
        CobustStream {
            receiver: Box::pin(receiver),
            task: cobust_task,
            task_done: false,
            panicked_items: Box::new(vec![]),
        }
    }
}

#[must_use = "CobustStream do nothing unless you start reading values out of it"]
pub struct CobustStream<RVAL: 'static, IVAL: 'static> {
    receiver: Pin<Box<Receiver<RVAL>>>,
    task: Pin<Box<dyn Future<Output = Vec<(IVAL, Option<()>)>>>>,
    task_done: bool,
    panicked_items: Box<Vec<IVAL>>,
}

impl<RVAL, IVAL> Stream for &mut CobustStream<RVAL, IVAL>
where
    IVAL: 'static,
    RVAL: 'static,
{
    type Item = RVAL;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.task_done {
            match self.task.as_mut().poll(cx) {
                Poll::Ready(failed_items) => {
                    self.panicked_items =
                        Box::new(failed_items.into_iter().map(|(item, _)| item).collect());
                    self.task_done = true;
                }
                Poll::Pending => {
                    // we have nothing to do here.
                }
            }
        }
        self.receiver.as_mut().poll_next(cx)
    }
}

impl<RVAL, IVAL> CobustStream<RVAL, IVAL>
where
    RVAL: 'static,
    IVAL: 'static,
{
    pub fn panicked_items(&self) -> &Vec<IVAL> {
        &self.panicked_items
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::panic::AssertUnwindSafe;

    use futures::StreamExt;
    use tokio;

    use crate::IntoCobust;

    #[tokio::test]
    async fn it_works() {
        let list: Vec<i32> = vec![2, 3];

        let mut cobust = list.into_iter().into_cobust(
            NonZeroUsize::new(1).unwrap(),
            1,
            || reqwest::Client::new(),
            |init_val, iter_val| {
                AssertUnwindSafe(async move {
                    if iter_val == 3 {
                        panic!("It's 3. I am panicking")
                    }
                    let client = init_val.0;

                    let response = client.get("https://google.com").send().await.unwrap();
                    let text = response.text().await.unwrap();

                    iter_val
                })
            },
        );

        cobust
            .for_each(|v| async move {
                println!("{:?}", v);
            })
            .await;

        println!("{:?} - failed", cobust.panicked_items());
    }
}
