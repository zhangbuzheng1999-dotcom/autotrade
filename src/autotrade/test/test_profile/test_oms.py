from time import sleep
from autotrade.engine.event_engine import (
    EVENT_ACCOUNT,
    EVENT_DATA,
    EVENT_ORDER,
    EVENT_POSITION_SNAPSHOT,
    EVENT_QUOTE,
    EVENT_TRADE,
    Event,
    EventEngine,
)
from autotrade.coreutils import Exchange,Product,Direction,OrderStatus
from autotrade.coreutils import TradeData,TickData,ContractData,PositionData,OrderData,AccountData,QuoteData
from autotrade.engine.oms import OmsBase
from autotrade.engine.security_manager import SecurityManager
from datetime import datetime
# 启动 EventEngine 和 OmsBase
event_engine = EventEngine()
oms = OmsBase(event_engine)
securities = SecurityManager(event_engine)
event_engine.start()

print("\n=== 测试行情更新 ===")
tick = TickData(gateway_name="SIM", symbol="RB99", exchange=Exchange.SHFE, datetime=datetime.now())
event_engine.put(Event(EVENT_DATA, tick))
sleep(0.05)
print("Tick:", securities.get_tick("RB99"))

print("\n=== 测试合约信息 ===")
contract = ContractData(gateway_name="SIM", symbol="RB99", exchange=Exchange.SHFE,
                        name="螺纹钢", product=Product.FUTURES, size=10, pricetick=1)
event_engine.put(Event(EVENT_DATA, contract))
sleep(0.05)
print("Contract:", securities.get_contract("RB99"))

print("\n=== 测试下单 + 激活状态 ===")
order = OrderData(gateway_name="SIM", symbol="RB99", exchange=Exchange.SHFE,
                  orderid="O001", price=3500, volume=2, status=OrderStatus.NOTTRADED)
event_engine.put(Event(EVENT_ORDER, order))
sleep(0.05)
print("Order:", oms.get_order("SIM.O001"))
print("Active Orders:", oms.get_all_active_orders())

print("\n=== 测试订单完成，活跃订单清除 ===")
order_done = OrderData(gateway_name="SIM", symbol="RB99", exchange=Exchange.SHFE,
                       orderid="O001", price=3500, volume=2, status=OrderStatus.ALLTRADED)
event_engine.put(Event(EVENT_ORDER, order_done))
sleep(0.05)
print("Active Orders:", oms.get_all_active_orders())

print("\n=== 测试成交记录 ===")
trade = TradeData(gateway_name="SIM", symbol="RB99", exchange=Exchange.SHFE,
                  orderid="O001", tradeid="T001", price=3500, volume=2,
                  direction=Direction.LONG, datetime=datetime.now())
event_engine.put(Event(EVENT_TRADE, trade))
sleep(0.05)
print("Trades:", oms.get_all_trades())

print("\n=== 测试持仓更新 ===")
pos = PositionData(gateway_name="SIM", symbol="RB99", exchange=Exchange.SHFE,
                   direction=Direction.LONG, volume=2, price=3500)
event_engine.put(Event(EVENT_POSITION_SNAPSHOT, pos))
pos = PositionData(gateway_name="SIM", symbol="AA99", exchange=Exchange.SHFE,
                   direction=Direction.LONG, volume=2, price=3500)
event_engine.put(Event(EVENT_POSITION_SNAPSHOT, pos))
sleep(0.05)
print("Position:", oms.get_position(pos.symbol))

print("\n=== 测试账户更新 ===")
account = AccountData(gateway_name="SIM", accountid="ACC001", balance=1_000_000)
event_engine.put(Event(EVENT_ACCOUNT, account))
sleep(0.05)
print("Account:", oms.get_account(account.vt_accountid))

print("\n=== 测试报价更新 ===")
quote = QuoteData(gateway_name="SIM", symbol="RB99", exchange=Exchange.SHFE,
                  quoteid="Q001", bid_price=3498, ask_price=3502)
event_engine.put(Event(EVENT_QUOTE, quote))
sleep(0.05)
print("Quote:", oms.get_quote("SIM.Q001"))
print("Active Quotes:", oms.get_all_active_quotes())
