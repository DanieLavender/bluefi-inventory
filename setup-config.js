// 일회용: .env의 API 키를 sync_config DB에 저장
require('dotenv').config();
const { initDb, query } = require('./database');

const configs = [
  ['store_a_client_id', '5vH0fcErhohGsvWInNNu9H'],
  ['store_a_client_secret', '$2a$04$tpAHaN9AGEaOs8Jbo6ljnO'],
  ['bulk_ftp_host', 'localhost'],
  ['bulk_ftp_port', '21'],
  ['bulk_ftp_user', 'Administrator'],
  ['bulk_ftp_password', 'HgMR7KBje9Bb'],
  ['bulk_ftp_path', '/FTP/Shoppingmall'],
  ['bulk_ftp_url_base', 'http://cosguardian.lavenderfriends.co.kr'],
];

(async () => {
  await initDb();
  for (const [key, value] of configs) {
    await query(
      'INSERT INTO sync_config (`key`, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)',
      [key, value]
    );
    console.log(`  ✓ ${key}`);
  }
  console.log('\n설정 저장 완료. 이 파일은 삭제해도 됩니다.');
  process.exit(0);
})();
