/**
 * Stable, hand-picked mainnet IDs used across UI specs.
 *
 * These mirror SAMPLE_PATH_VALUES in
 * test/bases/ccdexplorer/ccdexplorer_api/hand_picked_routes.py — that file is
 * the source of truth for individual values. If those values are updated (or
 * rotated because a record becomes unreachable), update both places together.
 *
 * Exception: BLOCK.hash. hand_picked_routes.py fills route params
 * independently per param name (for one-route-at-a-time smoke tests), so its
 * "block"/"block_hash" value does NOT actually correspond to
 * "height_or_hash": 30_000_000 — height and hash there are two unrelated
 * blocks. Some of these specs load the same block by both height and hash,
 * so BLOCK.hash below is the real hash for height 30,000,000 (verified
 * against a running site instance), not the hand_picked_routes.py value.
 */

export const NET = 'mainnet';

export const ACCOUNT = {
  address: '4hGN68SeYn9ZPSABU3uhS8nh8Tkv13DW2AmdZCneBzVkzeZ5Zp',
};

export const TX = {
  hash: 'e45cd77c71275c5d2c1e2a7aeacc6fd75870e2729511c6455dbd92a34aa976b5',
};

export const BLOCK = {
  hash: '7369bdc382fcffae5242e999f1f00485378b5d03ce71cd354167b61c5e70914a',
  height: 30_000_000,
};

export const CONTRACT = {
  index: 9882,
  subindex: 0,
};

export const MODULE = {
  ref: '8e31feffb4502800993e9efafa046e7c1244494dcc299eebd5e6d814a0d9d55f',
};

export const TOKEN = {
  id: '00001739',
};

export const NODE = {
  id: '8175412dde32cfab',
};

export const PROJECT = {
  id: 'aesirx',
};

export const WALLET = {
  index: 9645,
  subindex: 0,
  publicKey: '844ad4197a47afec6481d41472c49336209d8b3d762efd2b3e88c2587c60c1a7',
};
