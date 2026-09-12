use crate::error::AppError;
use crate::recovery::put_rec::put_recovery::PutRecovery;

pub(crate) struct Recovery;

impl Recovery {
    pub(crate) fn start() -> Result<(), AppError> {
        PutRecovery::start()?.recovery()
    }
}
