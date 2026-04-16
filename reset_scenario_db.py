"""
시나리오 DB 초기화 스크립트
=============================
잘못 쌓인 시나리오/노드를 전부 지우고 깨끗하게 시작.
news_alert.db (뉴스 수집 이력)는 건드리지 않습니다.

실행:
  python reset_scenario_db.py
  python reset_scenario_db.py --yes   # 확인 프롬프트 생략
"""

import os
import sys
import sqlite3

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scenario.db")


def main():
    if not os.path.exists(DB_PATH):
        print("scenario.db 파일이 없습니다. 초기화 불필요.")
        return

    conn = sqlite3.connect(DB_PATH)
    sc_count = conn.execute("SELECT COUNT(*) FROM scenarios").fetchone()[0]
    nd_count = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
    conn.close()

    print(f"현재 scenario.db: 시나리오 {sc_count}개 / 노드 {nd_count}개")

    if "--yes" not in sys.argv:
        ans = input("전부 삭제하고 초기화하시겠습니까? (yes 입력): ").strip()
        if ans.lower() != "yes":
            print("취소.")
            return

    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
        DELETE FROM nodes;
        DELETE FROM scenarios;
        DELETE FROM sqlite_sequence WHERE name IN ('scenarios', 'nodes');
    """)
    conn.commit()
    conn.close()
    print("✅ scenario.db 초기화 완료. 시나리오·노드 전부 삭제됨.")


if __name__ == "__main__":
    main()
