import logging
import time
from typing import Optional, Union

from core.app.apps.base_app_queue_manager import AppQueueManager
from core.app.entities.app_invoke_entities import AppGenerateEntity
from core.app.entities.queue_entities import QueueErrorEvent
from core.app.entities.task_entities import ErrorStreamResponse, PingStreamResponse, TaskState
from core.errors.error import QuotaExceededError
from core.model_runtime.errors.invoke import InvokeAuthorizationError, InvokeError
from core.moderation.output_moderation import ModerationRule, OutputModeration
from extensions.ext_database import db
from models.account import Account
from models.model import EndUser, Message

logger = logging.getLogger(__name__)

class BasedGenerateTaskPipeline:
    """
    BasedGenerateTaskPipeline is a class that generates stream output and manages state for the application.
    It includes AI-driven enhancements for anomaly detection and response enrichment.
    """

    _task_state: TaskState
    _application_generate_entity: AppGenerateEntity

    def __init__(self, application_generate_entity: AppGenerateEntity,
                 queue_manager: AppQueueManager,
                 user: Union[Account, EndUser],
                 stream: bool) -> None:
        """
        Initialize GenerateTaskPipeline.
        :param application_generate_entity: Application generate entity
        :param queue_manager: Queue manager
        :param user: User
        :param stream: Stream flag
        """
        self._application_generate_entity = application_generate_entity
        self._queue_manager = queue_manager
        self._user = user
        self._start_at = time.perf_counter()
        self._output_moderation_handler = self._init_output_moderation()
        self._stream = stream

        logger.info(f"Initialized BasedGenerateTaskPipeline for task_id: {self._application_generate_entity.task_id}")

    def _handle_error(self, event: QueueErrorEvent, message: Optional[Message] = None) -> Exception:
        """
        Handle error event and log details.
        :param event: Event
        :param message: Message
        :return: Exception
        """
        logger.debug("Handling error event: %s", event.error)
        e = event.error

        if isinstance(e, InvokeAuthorizationError):
            err = InvokeAuthorizationError('Incorrect API key provided')
        elif isinstance(e, InvokeError) or isinstance(e, ValueError):
            err = e
        else:
            err = Exception(e.description if getattr(e, 'description', None) is not None else str(e))

        if message:
            message = db.session.query(Message).filter(Message.id == message.id).first()
            err_desc = self._error_to_desc(err)
            message.status = 'error'
            message.error = err_desc

            db.session.commit()

        logger.error("Error handled: %s", err)
        return err

    @staticmethod
    def _error_to_desc(e: Exception) -> str:
        """
        Convert error to description.
        :param e: Exception
        :return: Error description
        """
        if isinstance(e, QuotaExceededError):
            return ("Your quota for Dify Hosted Model Provider has been exhausted. "
                    "Please go to Settings -> Model Provider to complete your own provider credentials.")

        message = getattr(e, 'description', str(e))
        if not message:
            message = 'Internal Server Error, please contact support.'

        return message

    def _error_to_stream_response(self, e: Exception) -> ErrorStreamResponse:
        """
        Convert error to stream response.
        :param e: Exception
        :return: ErrorStreamResponse
        """
        return ErrorStreamResponse(
            task_id=self._application_generate_entity.task_id,
            err=e
        )

    def _ping_stream_response(self) -> PingStreamResponse:
        """
        Ping stream response.
        :return: PingStreamResponse
        """
        return PingStreamResponse(task_id=self._application_generate_entity.task_id)

    def _init_output_moderation(self) -> Optional[OutputModeration]:
        """
        Initialize output moderation if sensitive word avoidance is enabled.
        :return: OutputModeration
        """
        app_config = self._application_generate_entity.app_config
        sensitive_word_avoidance = app_config.sensitive_word_avoidance

        if sensitive_word_avoidance:
            return OutputModeration(
                tenant_id=app_config.tenant_id,
                app_id=app_config.app_id,
                rule=ModerationRule(
                    type=sensitive_word_avoidance.type,
                    config=sensitive_word_avoidance.config
                ),
                queue_manager=self._queue_manager
            )

    def _handle_output_moderation_when_task_finished(self, completion: str) -> Optional[str]:
        """
        Handle output moderation when task is finished and return moderated completion text.
        :param completion: Completion text
        :return: Moderated completion text
        """
        if self._output_moderation_handler:
            self._output_moderation_handler.stop_thread()

            completion = self._output_moderation_handler.moderation_completion(
                completion=completion,
                public_event=False
            )

            self._output_moderation_handler = None

            return completion

        return None

    def _detect_anomalies(self, output: str) -> Optional[str]:
        """
        Detect anomalies in the generated output using AI.
        :param output: Generated output
        :return: Optional anomaly description
        """
        # Example AI-based anomaly detection
        # Implement actual anomaly detection logic here
        anomaly_description = None
        # Placeholder for anomaly detection logic
        if "suspicious" in output:
            anomaly_description = "Suspicious content detected in the output."

        if anomaly_description:
            logger.warning("Anomaly detected: %s", anomaly_description)
            return anomaly_description

        return None

    def _enrich_response(self, output: str) -> str:
        """
        Enrich the response based on AI insights.
        :param output: Generated output
        :return: Enriched response
        """
        anomaly_description = self._detect_anomalies(output)
        if anomaly_description:
            output += f"\nNote: {anomaly_description}"

        return output
