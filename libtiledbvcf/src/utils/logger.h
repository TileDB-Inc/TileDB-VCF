/**
 * @file   logger.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file defines class Logger.
 */

#pragma once

#ifndef TILEDB_LOGGER_H
#define TILEDB_LOGGER_H

#include <spdlog/fmt/ostr.h>
#include <spdlog/spdlog.h>

namespace tiledb {
namespace common {

/** Definition of class Logger. */
class Logger {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Logger();

  /** Destructor. */
  ~Logger();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Log a trace statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void trace(const char* msg) {
    logger_->trace(msg);
  }

  /**
   * A formatted trace statment.
   *
   * @param fmt A fmtlib format string, see http://fmtlib.net/latest/ for
   *     details.
   * @param arg positional argument to format.
   * @param args optional additional positional arguments to format.
   */
  template <typename Arg1, typename... Args>
  void trace(const char* fmt, const Arg1& arg1, const Args&... args) {
    logger_->trace(fmt, arg1, args...);
  }

  /**
   * Log a debug statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void debug(const char* msg) {
    logger_->debug(msg);
  }

  /**
   * A formatted debug statment.
   *
   * @param fmt A fmtlib format string, see http://fmtlib.net/latest/ for
   *     details.
   * @param arg positional argument to format.
   * @param args optional additional positional arguments to format.
   */
  template <typename Arg1, typename... Args>
  void debug(const char* fmt, const Arg1& arg1, const Args&... args) {
    logger_->debug(fmt, arg1, args...);
  }

  /**
   * Log an info statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void info(const char* msg) {
    logger_->info(msg);
  }

  /**
   * A formatted info statment.
   *
   * @param fmt A fmtlib format string, see http://fmtlib.net/latest/ for
   *     details.
   * @param arg positional argument to format.
   * @param args optional additional positional arguments to format.
   */
  template <typename Arg1, typename... Args>
  void info(const char* fmt, const Arg1& arg1, const Args&... args) {
    logger_->info(fmt, arg1, args...);
  }

  /**
   * Log a warn statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void warn(const char* msg) {
    logger_->warn(msg);
  }

  /**
   * A formatted warn statment.
   *
   * @param fmt A fmtlib format string, see http://fmtlib.net/latest/ for
   *     details.
   * @param arg positional argument to format.
   * @param args optional additional positional arguments to format.
   */
  template <typename Arg1, typename... Args>
  void warn(const char* fmt, const Arg1& arg1, const Args&... args) {
    logger_->warn(fmt, arg1, args...);
  }

  /**
   * Log an error with no message formatting.
   *
   * @param msg The string to log
   * */
  void error(const char* msg) {
    logger_->error(msg);
  }

  /** A formatted error statement.
   *
   * @param fmt A fmtlib format string, see http://fmtlib.net/latest/ for
   * details.
   * @param arg1 positional argument to format.
   * @param args optional additional positional arguments to format.
   */
  template <typename Arg1, typename... Args>
  void error(const char* fmt, const Arg1& arg1, const Args&... args) {
    logger_->error(fmt, arg1, args...);
  }

  /**
   * Log a critical statement with no message formatting.
   *
   * @param msg The string to log.
   */
  void critical(const char* msg) {
    logger_->critical(msg);
  }

  /**
   * A formatted critical statment.
   *
   * @param fmt A fmtlib format string, see http://fmtlib.net/latest/ for
   *     details.
   * @param arg positional argument to format.
   * @param args optional additional positional arguments to format.
   */
  template <typename Arg1, typename... Args>
  void critical(const char* fmt, const Arg1& arg1, const Args&... args) {
    logger_->critical(fmt, arg1, args...);
  }

  /** Verbosity level. */
  enum class Level : char {
    FATAL,
    ERR,
    WARN,
    INFO,
    DBG,
    TRACE,
  };

  /**
   * Set the logger level.
   *
   * @param lvl Logger::Level VERBOSE logs debug statements, ERR only logs
   *    Status Error's.
   */
  void set_level(Logger::Level lvl) {
    switch (lvl) {
      case Logger::Level::FATAL:
        logger_->set_level(spdlog::level::critical);
        break;
      case Logger::Level::ERR:
        logger_->set_level(spdlog::level::err);
        break;
      case Logger::Level::WARN:
        logger_->set_level(spdlog::level::warn);
        break;
      case Logger::Level::INFO:
        logger_->set_level(spdlog::level::info);
        break;
      case Logger::Level::DBG:
        logger_->set_level(spdlog::level::debug);
        break;
      case Logger::Level::TRACE:
        logger_->set_level(spdlog::level::trace);
        break;
      default:
        logger_->critical("Failed to set log level");
    }
    logger_level_ = lvl;
  }

  /**
   * Returns true if the logger is logging messages with the provided lvl.
   * Useful for checking if debug code should be executed.
   *
   * @param lvl The Logger::Level to check.
   */
  bool is_logging(Logger::Level lvl) {
    return logger_level_ >= lvl;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The logger object. */
  std::shared_ptr<spdlog::logger> logger_;
  Logger::Level logger_level_;
};

/* ********************************* */
/*              GLOBAL               */
/* ********************************* */

/** Global logger function. */
Logger& global_logger();

/** Set log level for global logger with enum. */
inline void LOG_SET_LEVEL(const Logger::Level lvl) {
  global_logger().set_level(lvl);
}

/** Set log level for global logger with int. */
inline void LOG_SET_LEVEL(const int lvl) {
  global_logger().set_level(static_cast<Logger::Level>(lvl));
}

/** Check if global logger is logging debug messages. */
inline bool LOG_DEBUG_ENABLED() {
  return global_logger().is_logging(Logger::Level::DBG);
}

/** Logs a trace message. */
inline void LOG_TRACE(const std::string& msg) {
  global_logger().trace(msg.c_str());
}

/** Logs a formatted trace message. */
template <typename Arg1, typename... Args>
inline void LOG_TRACE(const char* fmt, const Arg1& arg1, const Args&... args) {
  global_logger().trace(fmt, arg1, args...);
}

/** Logs a debug message. */
inline void LOG_DEBUG(const std::string& msg) {
  global_logger().debug(msg.c_str());
}

/** Logs a formatted debug message. */
template <typename Arg1, typename... Args>
inline void LOG_DEBUG(const char* fmt, const Arg1& arg1, const Args&... args) {
  global_logger().debug(fmt, arg1, args...);
}

/** Logs an info message. */
inline void LOG_INFO(const std::string& msg) {
  global_logger().info(msg.c_str());
}

/** Logs a formatted info message. */
template <typename Arg1, typename... Args>
inline void LOG_INFO(const char* fmt, const Arg1& arg1, const Args&... args) {
  global_logger().info(fmt, arg1, args...);
}

/** Logs a warning. */
inline void LOG_WARN(const std::string& msg) {
  global_logger().warn(msg.c_str());
}

/** Logs a formatted warning message. */
template <typename Arg1, typename... Args>
inline void LOG_WARN(const char* fmt, const Arg1& arg1, const Args&... args) {
  global_logger().warn(fmt, arg1, args...);
}

/** Logs an error. */
inline void LOG_ERROR(const std::string& msg) {
  global_logger().error(msg.c_str());
}

/** Logs a formatted error message. */
template <typename Arg1, typename... Args>
inline void LOG_ERROR(const char* fmt, const Arg1& arg1, const Args&... args) {
  global_logger().error(fmt, arg1, args...);
}

/** Logs a critical error and exits with a non-zero status. */
inline void LOG_FATAL(const std::string& msg) {
  global_logger().critical(msg.c_str());
  exit(1);
}

/** Logs a formatted critical error and exits with a non-zero status. */
template <typename Arg1, typename... Args>
inline void LOG_FATAL(const char* fmt, const Arg1& arg1, const Args&... args) {
  global_logger().critical(fmt, arg1, args...);
  exit(1);
}

}  // namespace common
}  // namespace tiledb

#endif  // TILEDB_LOGGER_H
