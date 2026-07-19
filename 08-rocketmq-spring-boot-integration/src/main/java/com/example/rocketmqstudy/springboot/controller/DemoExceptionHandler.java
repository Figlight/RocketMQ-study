package com.example.rocketmqstudy.springboot.controller;

import com.example.rocketmqstudy.springboot.model.ApiErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.validation.ConstraintViolationException;

/**
 * 学习接口的统一参数错误处理器。
 */
@RestControllerAdvice
public class DemoExceptionHandler {

    /**
     * 将请求体字段校验失败转换为统一的 400 响应。
     *
     * @param exception 请求体校验异常。
     * @return 错误响应。
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ApiErrorResponse handleBodyValidation(MethodArgumentNotValidException exception) {
        String message = exception.getBindingResult().getFieldErrors().stream()
                .findFirst()
                .map(error -> error.getDefaultMessage() == null ? "请求参数不合法" : error.getDefaultMessage())
                .orElse("请求参数不合法");
        return new ApiErrorResponse(message);
    }

    /**
     * 将查询参数校验及业务参数错误转换为统一的 400 响应。
     *
     * @param exception 参数异常。
     * @return 错误响应。
     */
    @ExceptionHandler({ConstraintViolationException.class, IllegalArgumentException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ApiErrorResponse handleParameterValidation(RuntimeException exception) {
        return new ApiErrorResponse(exception.getMessage());
    }
}
