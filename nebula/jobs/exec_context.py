# Names available to action conditions (create_if, start_if, skip_if), next
# to the Python built-ins. Conditions are written by the site administrator.
from nebula.enum import ContentType, JobState, MediaType, ObjectStatus, QCState

DEFAULT_EXEC_CONTEXT = {
    "ContentType": ContentType,
    "JobState": JobState,
    "MediaType": MediaType,
    "ObjectStatus": ObjectStatus,
    "QCState": QCState,
}
