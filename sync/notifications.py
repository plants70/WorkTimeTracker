import logging
from PyQt5.QtWidgets import QMessageBox
from PyQt5.QtCore import Qt

logger = logging.getLogger(__name__)

class Notifier:
    @staticmethod
    def show(title: str, message: str, parent=None):
        """Показывает системное уведомление или Qt-сообщение"""
        try:
            # Сначала пробуем показать системное уведомление
            try:
                from plyer import notification
                notification.notify(
                    title=title,
                    message=message,
                    app_name='WorkLog',
                    timeout=5
                )
                return
            except ImportError:
                logger.debug("Plyer не установлен, используем Qt-уведомления")
            except Exception as e:
                logger.warning("Ошибка системного уведомления", exc_info=e)

            # Fallback на Qt-сообщения
            msg = QMessageBox(parent)
            msg.setWindowFlags(Qt.WindowStaysOnTopHint)
            msg.setWindowTitle(title)
            msg.setText(message)
            msg.setIcon(QMessageBox.Information)
            msg.setStandardButtons(QMessageBox.Ok)
            msg.exec_()

        except Exception as e:
            logger.error("Ошибка показа уведомления", exc_info=e)
            logger.info("Уведомление (fallback): %s - %s", title, message)

    @staticmethod
    def show_warning(title: str, message: str, parent=None):
        """Показывает предупреждающее уведомление"""
        try:
            msg = QMessageBox(parent)
            msg.setWindowFlags(Qt.WindowStaysOnTopHint)
            msg.setWindowTitle(title)
            msg.setText(message)
            msg.setIcon(QMessageBox.Warning)
            msg.exec_()
        except Exception as e:
            logger.error("Ошибка показа предупреждения", exc_info=e)
            logger.warning("Предупреждение (fallback): %s - %s", title, message)

    @staticmethod
    def show_error(title: str, message: str, parent=None):
        """Показывает уведомление об ошибке"""
        try:
            msg = QMessageBox(parent)
            msg.setWindowFlags(Qt.WindowStaysOnTopHint)
            msg.setWindowTitle(title)
            msg.setText(message)
            msg.setIcon(QMessageBox.Critical)
            msg.exec_()
        except Exception as e:
            logger.error("Ошибка показа ошибки", exc_info=e)
            logger.error("Ошибка (fallback): %s - %s", title, message)
