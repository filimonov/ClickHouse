#pragma once

#include <memory>

namespace DB
{

class Context;
class CustomVariablesValuesDiskStorage;

using ContextMutablePtr = std::shared_ptr<Context>;

std::unique_ptr<CustomVariablesValuesDiskStorage> createCustomVariablesValuesStorage(const ContextMutablePtr & global_context);

}
